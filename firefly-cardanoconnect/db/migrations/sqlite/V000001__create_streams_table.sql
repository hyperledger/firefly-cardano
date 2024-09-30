CREATE TABLE "streams" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "batch_size" BIGINT NOT NULL,
    "batch_timeout" TEXT NOT NULL
);
CREATE UNIQUE INDEX "streams_name" ON "streams"("name");