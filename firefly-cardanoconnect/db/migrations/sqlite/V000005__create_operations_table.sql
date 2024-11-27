CREATE TABLE "operations" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "status" TEXT NOT NULL,
    "error_message" TEXT NULL,
    "tx_id" TEXT NULL
)