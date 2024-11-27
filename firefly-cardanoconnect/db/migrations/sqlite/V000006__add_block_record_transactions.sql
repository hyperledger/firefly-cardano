ALTER TABLE "block_records"
ADD COLUMN "transactions" BLOB NOT NULL
DEFAULT x'80'