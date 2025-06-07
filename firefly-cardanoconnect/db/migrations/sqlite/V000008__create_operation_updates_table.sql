CREATE TABLE "operation_updates" (
    "update_id" TEXT NOT NULL PRIMARY KEY,
    "id" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "error_message" TEXT NULL,
    "tx_id" TEXT NULL,
    "contract_address" TEXT NULL
);
