CREATE TABLE "stream_checkpoints" (
    "stream_id" TEXT NOT NULL PRIMARY KEY,
    "listeners" TEXT NOT NULL,
    FOREIGN KEY ("stream_id") REFERENCES "streams"("id") ON DELETE CASCADE
);