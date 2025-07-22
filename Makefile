default: run-worker

build-plugin:
	cd countmr && go build -buildmode=plugin -o ../build/

clean:
	rm -f  reducer_*

run-coordinator: 
	go run coordinator/coordinator.go txt/pg-*.txt

run-worker: build-plugin clean
	go run worker/worker.go build/countmr.so 
