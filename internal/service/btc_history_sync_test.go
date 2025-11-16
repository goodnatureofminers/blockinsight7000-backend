package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/golang/mock/gomock"
	"go.uber.org/zap"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/model"
)

func TestBTCHistorySyncServiceRun_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockBTCRepository(ctrl)
	rpc := NewMockBTCRpcClient(ctrl)
	logger := zap.NewNop()
	ctx := context.Background()

	service := NewBTCHistorySyncService(
		repo,
		rpc,
		"node",
		"network",
		logger,
		BTCHistoryBatchConfig{
			BlockBatchSize:  1,
			TxBatchSize:     1,
			OutputBatchSize: 1,
			InputBatchSize:  1,
		},
	)

	block := testBlock(0)

	repo.EXPECT().
		MaxBlockHeight(ctx, "node", "network").
		Return(uint64(0), false, nil)

	rpc.EXPECT().GetBlockCount().Return(int64(0), nil)
	rpc.EXPECT().GetBlockHash(int64(0)).Return(&chainhash.Hash{}, nil)
	rpc.EXPECT().GetBlockVerboseTx(gomock.Any()).Return(block, nil)

	repo.EXPECT().
		InsertBlocks(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, blocks []model.BTCBlock) error {
			if len(blocks) != 1 {
				t.Fatalf("expected 1 block, got %d", len(blocks))
			}
			if blocks[0].Height != 0 {
				t.Fatalf("unexpected height: %d", blocks[0].Height)
			}
			return nil
		})

	repo.EXPECT().
		InsertTransactions(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, txs []model.BTCTransaction) error {
			if len(txs) != 1 {
				t.Fatalf("expected 1 tx, got %d", len(txs))
			}
			if txs[0].TxID != "tx-0" {
				t.Fatalf("unexpected tx id: %s", txs[0].TxID)
			}
			return nil
		})

	repo.EXPECT().
		InsertTransactionOutputs(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, outputs []model.BTCTransactionOutput) error {
			if len(outputs) != 1 {
				t.Fatalf("expected 1 output, got %d", len(outputs))
			}
			if outputs[0].Value == 0 {
				t.Fatalf("expected non-zero output value")
			}
			return nil
		})

	repo.EXPECT().
		InsertTransactionInputs(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, inputs []model.BTCTransactionInput) error {
			if len(inputs) != 1 {
				t.Fatalf("expected 1 input, got %d", len(inputs))
			}
			if !inputs[0].IsCoinbase {
				t.Fatalf("expected coinbase input")
			}
			return nil
		})

	if err := service.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

func TestBTCHistorySyncServiceRun_InsertBlocksError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockBTCRepository(ctrl)
	rpc := NewMockBTCRpcClient(ctrl)
	logger := zap.NewNop()
	ctx := context.Background()

	service := NewBTCHistorySyncService(
		repo,
		rpc,
		"node",
		"network",
		logger,
		BTCHistoryBatchConfig{1, 1, 1, 1},
	)

	block := testBlock(0)

	repo.EXPECT().
		MaxBlockHeight(ctx, "node", "network").
		Return(uint64(0), false, nil)
	rpc.EXPECT().GetBlockCount().Return(int64(0), nil)
	rpc.EXPECT().GetBlockHash(int64(0)).Return(&chainhash.Hash{}, nil)
	rpc.EXPECT().GetBlockVerboseTx(gomock.Any()).Return(block, nil)

	expectedErr := errors.New("insert blocks failed")
	repo.EXPECT().
		InsertBlocks(ctx, gomock.Any()).
		Return(expectedErr)

	if err := service.Run(ctx); !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestBTCHistorySyncServiceRunAlreadyUpToDate(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockBTCRepository(ctrl)
	rpc := NewMockBTCRpcClient(ctrl)
	logger := zap.NewNop()
	ctx := context.Background()

	service := NewBTCHistorySyncService(
		repo,
		rpc,
		"node",
		"network",
		logger,
		DefaultBTCHistoryBatchConfig(),
	)

	repo.EXPECT().
		MaxBlockHeight(ctx, "node", "network").
		Return(uint64(5), true, nil)
	rpc.EXPECT().GetBlockCount().Return(int64(3), nil)

	if err := service.Run(ctx); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func testBlock(height uint64) *btcjson.GetBlockVerboseTxResult {
	ts := time.Unix(1_710_000_000, 0)
	return &btcjson.GetBlockVerboseTxResult{
		Hash:       fmt.Sprintf("hash-%d", height),
		Height:     int64(height),
		Bits:       "1d00ffff",
		Size:       1,
		Version:    1,
		Time:       ts.Unix(),
		Difficulty: 1,
		Nonce:      1,
		Tx: []btcjson.TxRawResult{
			{
				Txid:     fmt.Sprintf("tx-%d", height),
				Version:  1,
				LockTime: 0,
				Size:     1,
				Vsize:    1,
				Vin: []btcjson.Vin{
					{
						Coinbase: "coinbase-data",
						Sequence: 0,
					},
				},
				Vout: []btcjson.Vout{
					{
						Value: 6.25,
						N:     0,
						ScriptPubKey: btcjson.ScriptPubKeyResult{
							Type:      "pubkeyhash",
							Hex:       "00",
							Asm:       "asm",
							Addresses: []string{"addr1"},
						},
					},
				},
			},
		},
	}
}
