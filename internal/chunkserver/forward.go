package chunkserver

import (
	"context"
	"fmt"
	"sync"

	gfsv1 "github.com/aayush0325/gofs/gen/gfs/v1"
)

// forwardWrites sends a write mutation to every secondary in parallel.
func (s *Service) forwardWrites(ctx context.Context, req *gfsv1.WriteChunkRequest) error {
	return s.forward(ctx, req.Secondaries, func(client gfsv1.ChunkServerClient, ctx context.Context, address string) error {
		_, err := client.WriteChunk(ctx, &gfsv1.WriteChunkRequest{
			Handle: req.Handle, Version: req.Version, Offset: req.Offset, Data: req.Data,
			MutationId: req.MutationId, Forward: true,
		})
		return err
	})
}

// forwardAppends sends an append at the primary-selected offset to every
// secondary in parallel.
func (s *Service) forwardAppends(ctx context.Context, req *gfsv1.AppendChunkRequest) error {
	return s.forward(ctx, req.Secondaries, func(client gfsv1.ChunkServerClient, ctx context.Context, address string) error {
		_, err := client.AppendChunk(ctx, &gfsv1.AppendChunkRequest{
			Handle: req.Handle, Version: req.Version, Offset: req.Offset, Data: req.Data,
			MutationId: req.MutationId, Forward: true,
		})
		return err
	})
}

// forward runs a mutation against all secondaries and waits for all attempts so
// retries see a stable set of applied replicas.
func (s *Service) forward(ctx context.Context, replicas []*gfsv1.Replica, call func(gfsv1.ChunkServerClient, context.Context, string) error) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(replicas))
	for _, replica := range replicas {
		replica := replica
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := s.pool.Client(ctx, replica.Address)
			if err != nil {
				errs <- fmt.Errorf("%s: %w", replica.Address, err)
				return
			}
			if err := call(client, ctx, replica.Address); err != nil {
				errs <- fmt.Errorf("%s: %w", replica.Address, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}
