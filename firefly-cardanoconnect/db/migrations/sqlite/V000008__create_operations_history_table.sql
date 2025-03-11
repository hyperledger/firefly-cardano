CREATE TABLE "operations_history" (
    "id" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "error_message" TEXT NULL,
    "tx_id" TEXT NULL,
    "contract_address" TEXT NULL,
    "timestamp" TEXT NOT NULL,
    PRIMARY KEY ("id", "timestamp")
);

CREATE INDEX "operations_history_timestamp" ON "operations_history"("timestamp");
