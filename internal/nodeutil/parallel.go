package nodeutil

import "sync"

func RunParallel[T any, R any](limit int, requests []T, fn func(int, T) (R, error)) ([]R, error) {
	results := make([]R, len(requests))
	if len(requests) == 0 {
		return results, nil
	}
	if limit <= 0 || limit > len(requests) {
		limit = len(requests)
	}

	type asyncResult struct {
		index  int
		result R
		err    error
	}

	sem := make(chan struct{}, limit)
	resultCh := make(chan asyncResult, len(requests))
	var wg sync.WaitGroup
	for i, request := range requests {
		wg.Add(1)
		go func(index int, request T) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			result, err := fn(index, request)
			resultCh <- asyncResult{
				index:  index,
				result: result,
				err:    err,
			}
		}(i, request)
	}

	wg.Wait()
	close(resultCh)

	for result := range resultCh {
		if result.err != nil {
			return nil, result.err
		}
		results[result.index] = result.result
	}
	return results, nil
}
