package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/stretchr/testify/suite"
	tcClickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

const (
	clickhouseImage = "clickhouse/clickhouse-server:25.11"
)

type RepositorySuite struct {
	suite.Suite
	ctx        context.Context
	cancel     context.CancelFunc
	container  *tcClickhouse.ClickHouseContainer
	dsn        string
	repo       *Repository
	metrics    *MockMetrics
	metricsCtl *gomock.Controller
	testCtx    context.Context
	testCancel context.CancelFunc
}

func TestRepositorySuite(t *testing.T) {
	suite.Run(t, new(RepositorySuite))
}

func (s *RepositorySuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)

	container, err := tcClickhouse.Run(s.ctx,
		clickhouseImage,
		tcClickhouse.WithUsername("default"),
		tcClickhouse.WithDatabase("default"),
	)
	s.Require().NoError(err)

	s.container = container

	dsn, err := container.ConnectionString(s.ctx)
	s.Require().NoError(err)
	s.dsn = dsn
}

func (s *RepositorySuite) TearDownSuite() {
	if s.container != nil {
		_ = s.container.Terminate(context.Background())
	}
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *RepositorySuite) SetupTest() {
	s.testCtx, s.testCancel = context.WithTimeout(context.Background(), time.Minute)
	s.metricsCtl = gomock.NewController(s.T())
	s.metrics = NewMockMetrics(s.metricsCtl)

	s.Require().NoError(applyMigrationsUp(s.dsn))

	repo, err := NewRepository(s.dsn, s.metrics)
	s.Require().NoError(err)
	s.repo = repo
}

func (s *RepositorySuite) TearDownTest() {
	if s.testCancel != nil {
		s.testCancel()
	}
	s.Require().NoError(applyMigrationsDown(s.dsn))
	if s.metricsCtl != nil {
		s.metricsCtl.Finish()
	}
}

func newBlock(status model.BlockStatus, height uint64, suffix string, ts time.Time) model.Block {
	return model.Block{
		Coin:       model.BTC,
		Network:    model.Mainnet,
		Height:     height,
		Hash:       strings.Repeat(suffix, 64/len(suffix)),
		Timestamp:  ts,
		Version:    1,
		MerkleRoot: strings.Repeat("f", 64),
		Bits:       1,
		Nonce:      1,
		Difficulty: 1,
		Size:       100,
		TXCount:    1,
		Status:     status,
	}
}

func (s *RepositorySuite) countRows(table string) uint64 {
	rows, err := s.repo.conn.Query(s.testCtx, fmt.Sprintf("SELECT count() FROM %s", table))
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(rows.Close())
	}()

	var count uint64
	s.Require().True(rows.Next())
	s.Require().NoError(rows.Scan(&count))
	return count
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working dir: %w", err)
	}

	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			return "", fmt.Errorf("go.mod not found from %s", dir)
		}
		dir = next
	}
}

func applyMigrationsUp(dsn string) error {
	m, err := newMigrator(dsn)
	if err != nil {
		return err
	}
	defer func() {
		_ = closeMigrator(m)
	}()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

func applyMigrationsDown(dsn string) error {
	m, err := newMigrator(dsn)
	if err != nil {
		return err
	}
	defer func() {
		_ = closeMigrator(m)
	}()

	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate down: %w", err)
	}
	return nil
}

func newMigrator(dsn string) (*migrate.Migrate, error) {
	root, err := moduleRoot()
	if err != nil {
		return nil, err
	}

	sourceURL := fmt.Sprintf("file://%s", filepath.Join(root, "migrations", "clickhouse"))
	targetDSN := withMultiStatement(dsn)
	m, err := migrate.New(sourceURL, targetDSN)
	if err != nil {
		return nil, fmt.Errorf("init migrate: %w", err)
	}
	return m, nil
}

func withMultiStatement(dsn string) string {
	if strings.Contains(dsn, "x-multi-statement=") {
		return dsn
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "x-multi-statement=true"
}

func closeMigrator(m *migrate.Migrate) error {
	if m == nil {
		return nil
	}
	sourceErr, dbErr := m.Close()
	if sourceErr != nil && dbErr != nil {
		return fmt.Errorf("close migrator: source: %v; database: %v", sourceErr, dbErr)
	}
	if sourceErr != nil {
		return fmt.Errorf("close migrator: source: %w", sourceErr)
	}
	if dbErr != nil {
		return fmt.Errorf("close migrator: database: %w", dbErr)
	}
	return nil
}

func (s *RepositorySuite) seedBlocks(blocks []model.Block) {
	batch, err := s.repo.conn.PrepareBatch(s.testCtx, `
INSERT INTO utxo_blocks (
    coin,
    network,
    height,
    hash,
    timestamp,
    version,
    merkleroot,
    bits,
    nonce,
    difficulty,
    size,
    tx_count,
    status
) VALUES`)
	s.Require().NoError(err)

	for _, b := range blocks {
		err = batch.Append(
			string(b.Coin),
			string(b.Network),
			b.Height,
			b.Hash,
			b.Timestamp,
			b.Version,
			b.MerkleRoot,
			b.Bits,
			b.Nonce,
			b.Difficulty,
			b.Size,
			b.TXCount,
			string(b.Status),
		)
		s.Require().NoError(err)
	}
	s.Require().NoError(batch.Send())
}

func (s *RepositorySuite) seedTransactions(txs []model.Transaction) {
	batch, err := s.repo.conn.PrepareBatch(s.testCtx, `
INSERT INTO utxo_transactions (
    coin,
    network,
    txid,
    block_height,
    timestamp,
    size,
    vsize,
    version,
    locktime,
    input_count,
    output_count
) VALUES`)
	s.Require().NoError(err)

	for _, tx := range txs {
		err = batch.Append(
			string(tx.Coin),
			string(tx.Network),
			tx.TxID,
			tx.BlockHeight,
			tx.Timestamp,
			tx.Size,
			tx.VSize,
			tx.Version,
			tx.LockTime,
			tx.InputCount,
			tx.OutputCount,
		)
		s.Require().NoError(err)
	}
	s.Require().NoError(batch.Send())
}

func (s *RepositorySuite) seedTransactionOutputs(outputs []model.TransactionOutput) {
	batch, err := s.repo.conn.PrepareBatch(s.testCtx, `
INSERT INTO utxo_transaction_outputs (
    coin,
    network,
    block_height,
    block_timestamp,
    txid,
    output_index,
    value,
    script_type,
    script_hex,
    script_asm,
    addresses
) VALUES`)
	s.Require().NoError(err)

	for _, output := range outputs {
		err = batch.Append(
			string(output.Coin),
			string(output.Network),
			output.BlockHeight,
			output.BlockTime,
			output.TxID,
			output.Index,
			output.Value,
			output.ScriptType,
			output.ScriptHex,
			output.ScriptAsm,
			output.Addresses,
		)
		s.Require().NoError(err)
	}
	s.Require().NoError(batch.Send())
}
