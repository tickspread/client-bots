export BINANCE_KEY=AvS2i0ITHY1j2rjIusBTFGUDwtr8pdLBcJ89MfhRS6YchBhHT35dQSVrxR3kqVAb
export BINANCE_SECRET=9fSAg39j9gcguw8kSTj9utmLoeghTN4nXt5kolH7f0w1gpVDYgQ9Gy5KjTYWPlMn

./kafka-configs.sh --zookeeper 127.0.0.1:2021  --entity-type topics --alter --add-config cleanup.policy=delete,delete.retention.ms=43200000 --entity-name "order-book"