.PHONY: build
build:
	env GOOS=linux go build -o ./bin/go_collect_log

.PHONY: deploy
deploy: build
	cp bin/go_collect_log roles/go_collect_log/files
	ansible-playbook go_collect_log.yml