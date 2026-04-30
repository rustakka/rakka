# **Rakka Persistence Providers Plan**

## **1\. Analysis of Akka.NET Persistence Repositories**

The akkadotnet organization has historically maintained separate plugins for different databases, but recently moved toward a unified SQL approach. The key persistence backend repositories to port are:

### **SQL Repositories**

* **Akka.Persistence.Sql** (The modern, unified SQL plugin using Linq2Db)  
* *Legacy distinct plugins:* Akka.Persistence.SqlServer, Akka.Persistence.PostgreSql, Akka.Persistence.MySql, Akka.Persistence.Sqlite.

### **NoSQL & Cloud Repositories**

* **Akka.Persistence.Redis**  
* **Akka.Persistence.MongoDB**  
* **Akka.Persistence.Cassandra**  
* **Akka.Persistence.Azure** (Table Storage / CosmosDB)  
* **Akka.Persistence.AWS** (DynamoDB)

## **2\. Implementation Plan for rakka**

To implement these in Rust efficiently, we should skip the legacy distinct SQL plugins and aim straight for a unified SQL provider, leveraging Rust's powerful sqlx crate, alongside separate crates for NoSQL databases.

### **A. Crate Structure (Cargo Workspace)**

Set up a Cargo workspace within the rakka repository (or a dedicated rakka-persistence repo) to manage the providers:

* rakka-persistence (Core traits: AsyncWriteJournal, SnapshotStore, ReadJournal)  
* rakka-persistence-sql (Unified SQL provider for Postgres, MySQL, SQLite, MSSQL)  
* rakka-persistence-redis  
* rakka-persistence-mongodb  
* rakka-persistence-cassandra

### **B. Rust Ecosystem Mapping**

Instead of C\# libraries, we will map Akka.NET's dependencies to Rust's native asynchronous ecosystem:

* **Async Runtime:** tokio (assuming rakka uses it).  
* **SQL:** sqlx (allows raw async queries with connection pooling for Postgres, MySQL, SQLite, and MSSQL).  
* **Redis:** redis (or fred for an advanced async Redis client).  
* **MongoDB:** mongodb (official async MongoDB Rust driver).  
* **Cassandra:** scylla (highly performant Rust driver by ScyllaDB, fully compatible with Cassandra).

### **C. Phased Rollout**

1. **Phase 1: Core Definitions & In-Memory**  
   * Define the core Journal and SnapshotStore Rust traits using async\_trait (or native async traits if using Rust 1.75+).  
   * Implement rakka-persistence-memory for testing.  
2. **Phase 2: Unified SQL Provider (rakka-persistence-sql)**  
   * Implement the journal and snapshot store using sqlx.  
   * Start with SQLite (easiest for local CI testing).  
   * Extend schema generation and tests to Postgres and MySQL.  
3. **Phase 3: High-Performance NoSQL (redis & mongodb)**  
   * Implement rakka-persistence-redis (prioritize due to high throughput use cases).  
   * Implement rakka-persistence-mongodb (JSON/BSON document storage mapping).  
4. **Phase 4: Distributed / Cloud Plugins**  
   * Implement Cassandra, AWS DynamoDB, and Azure.

## **3\. GitHub Release Strategy**

Once the crates are implemented, releasing them via GitHub Actions and crates.io requires a synchronized workflow.

### **Step 1: Versioning Strategy**

Use a tool like [cargo-release](https://github.com/crate-ci/cargo-release) to manage workspace versions. When a milestone is reached, update the version in the main Cargo.toml and let cargo-release cascade it to all provider crates.

### **Step 2: GitHub Actions CI/CD Pipeline**

Create a file at .github/workflows/release.yml in your repository. This action will trigger automatically whenever you publish a GitHub Release.  
`name: Publish Crates`

`on:`  
  `release:`  
    `types: [published]`

`jobs:`  
  `publish:`  
    `runs-on: ubuntu-latest`  
    `steps:`  
      `- name: Checkout code`  
        `uses: actions/checkout@v4`

      `- name: Install Rust toolchain`  
        `uses: dtolnay/rust-toolchain@stable`

      `- name: Verify Builds and Tests`  
        `run: cargo test --workspace --all-features`

      `- name: Cargo Publish Core`  
        `env:`  
          `CARGO_REGISTRY_TOKEN: ${{ secrets.CRATES_IO_TOKEN }}`  
        `run: cargo publish -p rakka-persistence`

      `- name: Cargo Publish SQL`  
        `env:`  
          `CARGO_REGISTRY_TOKEN: ${{ secrets.CRATES_IO_TOKEN }}`  
        `run: cargo publish -p rakka-persistence-sql`

      `# Add additional publish steps for redis, mongodb, etc.`

### **Step 3: Steps to Execute a Release**

1. **Update Changelog:** Update the CHANGELOG.md detailing new persistence providers or fixes.  
2. **Bump Versions:** Run cargo release version minor (or major/patch) locally to bump versions across the workspace, commit, and push.  
3. **Draft a Release on GitHub:**  
   * Go to your rakka GitHub repo \-\> **Releases** \-\> **Draft a new release**.  
   * Create a new tag (e.g., v0.2.0).  
   * Title the release and paste the changelog notes.  
4. **Publish:** Click **Publish release**.  
5. **Automation Takes Over:** The GitHub Action will detect the published event, run your test suite against the DBs, and automatically authenticate and publish the new provider crates to crates.io.