

.PHONY: export
export:
	cargo run --target x86_64-apple-darwin -- export

.PHONY: view-setup
view-setup:
	cargo run --target x86_64-apple-darwin -- setup view