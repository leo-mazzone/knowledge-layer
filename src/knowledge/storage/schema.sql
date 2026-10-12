CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS knowledge_schema (
    version integer PRIMARY KEY
);
INSERT INTO knowledge_schema VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id uuid PRIMARY KEY,
    connector text NOT NULL,
    collection text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'running',
    summary jsonb NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS documents (
    id uuid PRIMARY KEY,
    source text NOT NULL,
    source_id text NOT NULL,
    UNIQUE (source, source_id)
);

CREATE TABLE IF NOT EXISTS document_versions (
    id uuid PRIMARY KEY,
    document_id uuid NOT NULL REFERENCES documents(id),
    fingerprint text NOT NULL,
    metadata jsonb NOT NULL,
    extracted_text text NOT NULL,
    raw_content jsonb NOT NULL,
    embedding_signature text NOT NULL,
    processing jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (document_id, fingerprint),
    UNIQUE (document_id, id)
);

CREATE TABLE IF NOT EXISTS collection_documents (
    collection text NOT NULL,
    document_id uuid NOT NULL REFERENCES documents(id),
    version_id uuid NOT NULL,
    PRIMARY KEY (collection, document_id),
    FOREIGN KEY (document_id, version_id) REFERENCES document_versions(document_id, id)
);

CREATE TABLE IF NOT EXISTS chunks (
    version_id uuid NOT NULL REFERENCES document_versions(id),
    ordinal integer NOT NULL,
    text text NOT NULL,
    heading_path jsonb NOT NULL,
    locator text,
    embedding vector NOT NULL,
    search_text tsvector NOT NULL,
    PRIMARY KEY (version_id, ordinal)
);
CREATE INDEX IF NOT EXISTS chunks_full_text ON chunks USING gin(search_text);
CREATE INDEX IF NOT EXISTS collection_documents_version ON collection_documents(version_id);
-- Exact vector search is the initial retrieval strategy. A later ANN migration
-- needs a fixed-dimension index per embedding configuration and a recall evaluation.
