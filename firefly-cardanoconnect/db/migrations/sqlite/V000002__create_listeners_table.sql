CREATE TABLE "listeners" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "stream_id" TEXT NOT NULL,
    "filters" TEXT NOT NULL,
    FOREIGN KEY ("stream_id") REFERENCES "streams"("id") ON DELETE CASCADE
);
CREATE UNIQUE INDEX "listeners_stream_id_name" ON "listeners"("stream_id", "name");