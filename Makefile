.PHONY: export
export:
	RUST_LOG=info cargo run --target x86_64-apple-darwin -- export

.PHONY: view-setup
view-setup:
	RUST_LOG=info cargo run --target x86_64-apple-darwin -- setup view

.PHONY: build
build:
	cargo build --release --target x86_64-apple-darwin