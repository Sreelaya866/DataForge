# DataForge

An end-to-end Big Data Analytics and AutoML platform. Upload a CSV and DataForge stores it in **Hadoop HDFS**, cleans it with **Apache Spark** plus a second cleaning pass, trains and ranks seven algorithms with **AutoML**, explains the best model with **SHAP**, and serves live predictions. A **Live Backend** panel shows the real code and output of every step in the browser.

The project novelty is **Smart AutoML**: an optimized engine (screening, Bayesian search with Optuna, early stopping, time budget) compared fairly against a baseline engine.


**Note on `[confirm]` markers:** items marked `[confirm]` are written from the design of the project but were not checked against the final source files. Verify each one against your code (or the interactive API page at `http://localhost:8000/docs`) before submitting.

---

## Contents

1. Features
2. Architecture and pipeline
3. Project layout
4. Requirements
5. Installation
6. Configuration (.env)
7. Running the project
8. Docker Compose
9. HDFS setup
10. Using the app (demo walkthrough)
11. How AutoML and Smart AutoML work
12. Data cleaning rules
13. Live Backend panel
14. API reference
15. Database schema
16. Security notes
17. Testing
18. Git workflow for the team
19. Troubleshooting
20. Limitations and future work
21. Team, license and credits

---

## 1. Features

- JWT login, each user sees only their own datasets and models
- Upload to HDFS (WebHDFS), automatic fallback to local disk if Hadoop is down
- Spark profiling and cleaning, then an extra cleaning pass (units to numbers, row-number / ID / free-text columns dropped, duplicates removed)
- AutoML with 7 algorithms: Logistic or Linear Regression, Decision Tree, Random Forest, Gradient Boosting, XGBoost, SVM, Neural Network
- Classification (accuracy, F1) and regression (RMSE, MAE) detected automatically from the target column
- Smart AutoML vs baseline comparison with a time budget
- SHAP feature importance for the best model
- Prediction form that follows the model's dataset, hides the target column and can **fill random values**
- Statistics panel with a column picker and PDF export
- Live Backend panel streaming API, SQL, Upload, HDFS, Spark, AutoML, SHAP and Predict events over SSE
- Seven pages: Dashboard, Datasets, Training, Models, Predictions, History, Settings

## 2. Architecture and pipeline

```
Browser (single HTML + JS)
        |  REST + SSE (JWT in Authorization header)
        v
FastAPI (app/main.py) ── routers: auth, datasets, training, models, predictions, live
        |
        ├── HDFS (WebHDFS)      raw and cleaned files
        ├── Spark               profiling and basic cleaning
        ├── data_cleaning.py    second cleaning pass
        ├── AutoML engines      baseline and optimized
        ├── SHAP                explanations
        ├── PostgreSQL          users, datasets, runs, models, predictions
        └── Redis (or file)     live event feed
```

Pipeline for one dataset:

1. **Upload**: the API saves a dataset record and the file.
2. **Raw to HDFS**: stored at `/dataforge/raw/<dataset_id>/<file>`.
3. **Spark**: profiles the data (types, missing values) and cleans it.
4. **Extra cleaning**: `data_cleaning.refine()` (see section 12).
5. **Cleaned to HDFS**: stored at `/dataforge/cleaned/<dataset_id>/cleaned.csv`.
6. **Training**: baseline or optimized AutoML trains and ranks models, saves them, and SHAP explains the best one.
7. **Prediction**: the saved model is loaded and returns a result.

Jobs run on background threads (`app/jobs.py`), with a structure that can move to Celery workers. [confirm]

## 3. Project layout

```
dataforge/
├─ app/
│  ├─ main.py                 API entry point, live logging middleware, startup HDFS probe
│  ├─ config.py               settings read from .env [confirm]
│  ├─ database.py             SQLAlchemy engine, session, Base [confirm]
│  ├─ models.py               database tables [confirm]
│  ├─ schemas.py              request/response models [confirm]
│  ├─ jobs.py                 run_in_background helper
│  ├─ pipeline.py             preprocessing and training jobs
│  ├─ routers/
│  │  ├─ auth.py              register, login
│  │  ├─ datasets.py          upload, list, stats, HDFS info
│  │  ├─ training.py          start runs, benchmark
│  │  ├─ models.py            list and detail
│  │  ├─ predictions.py       predict, history
│  │  └─ live.py              live feed (recent, stream, clear, hdfs)
│  └─ services/
│     ├─ hdfs_client.py       WebHDFS client (never raises)
│     ├─ spark_processing.py  Spark profiling and cleaning
│     ├─ data_cleaning.py     second cleaning pass
│     ├─ automl.py            baseline engine and shared helpers
│     ├─ automl_optimized.py  Smart AutoML engine
│     ├─ explain.py           SHAP
│     ├─ storage.py           local/HDFS storage helpers [confirm]
│     └─ live.py              live event feed, SQL logging, auto-tracing
├─ frontend/dataforge-frontend-connected.html
├─ docs/HDFS_SETUP.md
├─ docker-compose.yml
├─ requirements.txt
├─ .env.example
└─ README.md
```

## 4. Requirements

| Item | Version | Needed for |
|---|---|---|
| Python | 3.11 or later | Backend |
| PostgreSQL | 14 or later [confirm] | Records (Docker Compose provides it) |
| Redis | any recent | Live feed (optional: a file is used if Redis is absent) |
| Java | 17 | Hadoop and Spark |
| Hadoop | 3.5.0 | Real HDFS (optional) |
| Docker Desktop | recent | Compose setup (optional) |

Tested on a MacBook Air (Apple silicon) with Homebrew.

## 5. Installation

```bash
git clone <repository-url>
cd dataforge
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install requests redis
cp .env.example .env
```
Then fill in `.env` (section 6). If `requirements.txt` is missing a package you hit an `ImportError` for, `pip install` it and add it to the file.

## 6. Configuration (.env)

Never commit `.env`. Share its values privately with teammates. Names below are the project's settings; check them against `.env.example` and `app/config.py`. [confirm]

| Variable | Example | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql://user:password@localhost:5432/dataforge` | Database connection |
| `SECRET_KEY` | long random string | Signs JWT tokens |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | `60` | Token lifetime |
| `CORS_ORIGINS` | `http://localhost:5500` | Allowed frontend origins |
| `REDIS_URL` | `redis://localhost:6379/0` | Live event feed; file used if unreachable |
| `HDFS_WEBHDFS_URL` | `http://localhost:9870` | NameNode WebHDFS address |
| `HDFS_USER` | your Mac username | HDFS user (must match the user that formatted the NameNode) |
| `HDFS_ROOT` | `/dataforge` | Root folder in HDFS |
| `HDFS_ENABLED` | `true` | `false` forces local storage only |

Generate a secret: `python3 -c "import secrets; print(secrets.token_urlsafe(48))"`.

## 7. Running the project

Use two terminal tabs.

**Backend (tab 1)**

```bash
cd dataforge
source venv/bin/activate
uvicorn app.main:app --reload --port 8000
```
Check it: `curl -s http://localhost:8000/api/health` should print `{"status":"ok"}`. API docs: `http://localhost:8000/docs`.

**Frontend (tab 2)**

```bash
cd dataforge/frontend
python3 -m http.server 5500
```
Open `http://localhost:5500/dataforge-frontend-connected.html` and hard refresh (Cmd+Shift+R). If your folder name has spaces, put the path in quotes.

## 8. Docker Compose

`docker-compose.yml` starts PostgreSQL, Redis, the API and a Celery worker. [confirm services]

```bash
docker compose up --build        # start everything
docker compose ps                # check status
docker compose logs -f api       # follow API logs
docker compose down              # stop
```
Compose runs without real HDFS unless Hadoop is reachable from the containers; uploads then use local storage inside the container.

## 9. HDFS setup

Full step-by-step instructions are in `docs/HDFS_SETUP.md`. Summary:

```bash
brew install hadoop
# configure core-site.xml and hdfs-site.xml (see the guide), then once:
hdfs namenode -format -force
# every session:
hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs dfs -mkdir -p /dataforge/raw /dataforge/cleaned
hdfs dfs -ls -R /dataforge
```
Stop with `hdfs --daemon stop datanode; hdfs --daemon stop namenode`. The app shows an "HDFS connected" or "HDFS offline" pill in the Live panel.

## 10. Using the app (demo walkthrough)

1. **Sign up and log in.**
2. **Datasets**: Upload dataset. Wait for status **Ready**. The raw and cleaned files appear in HDFS.
3. **Statistics**: open a dataset's stats; pick which columns to chart or export to PDF.
4. **Train**: choose the dataset, the **target column** and the engine (baseline or optimized). Training one target does not train the others; the other columns are used as inputs.
5. **Live backend** (button bottom-right): use the **Key steps** filter to hide SQL noise. Filter by AutoML, SHAP, HDFS and so on.
6. **Models**: leaderboard, baseline vs optimized comparison, best model and metrics.
7. **Predictions**: pick a model, click **Fill random values** (or type values), then Predict.
8. **History** and **Settings** show earlier runs and account options.

Suggested dataset for evaluation: Diamonds (target `price`, regression). Good targets are ones the data can really predict; a target such as Country in a survey dataset will not be learnable.

## 11. How AutoML and Smart AutoML work

**Baseline** (`automl.py`): shared train/test split and preprocessing, then RandomizedSearchCV for each of the seven algorithms, scoring and ranking all of them.

**Smart AutoML** (`automl_optimized.py`):

1. **Screening**: 3-fold cross-validation of every algorithm on up to 3,000 rows.
2. **Selection**: keep the best half (at least 4 of 7) as finalists.
3. **Bayesian search**: Optuna TPE for each finalist, 5 to 10 trials; trial 0 is the library default, so tuning never ends worse than defaults.
4. **Early stopping**: stop after 4 trials without improvement (never before 5 trials) or at the time limit.
5. **Time budget**: 20 s per run for under 5,000 rows, 60 s for under 50,000, 120 s above.
6. **Final fit** on the full training data, test metrics, ranking.

**Fair comparison**: both engines share the same split, preprocessing, metrics, ranking, random seed and parallelism setting. Ties are broken the same way every run: test metric, then CV score, then training time, then algorithm name. Differences of one or two test rows (for example 0.0125 on a 400-row file) are noise.

**Task detection**: numeric targets with many distinct values are treated as regression; few distinct values or text labels as classification. XGBoost handles text class labels through an internal label encoder.

Reference results from development tests: about 1.9x faster on a 400-row churn file and 1.0 to 1.3x on other datasets, with equal accuracy within noise. Your own numbers will differ; record them in the report.

## 12. Data cleaning rules

After Spark, `data_cleaning.refine()` rewrites the cleaned CSV:

1. Drop a saved row-number column ("Unnamed: 0", or a 0..n-1 / 1..n counter).
2. Convert text columns where at least 90% of values are numbers with units or symbols ("16 yrs", "$1,884", "2 hrs").
3. Drop identifier columns (almost every value different) and long free-text columns.
4. Remove duplicate rows.

Each decision is shown in the Live panel, and a failure never stops the pipeline. Outliers (for example an age of 97) are left in place.

## 13. Live Backend panel

- Events come from the backend with a stage, a title, the code that ran and its output.
- Transport: Redis if reachable, otherwise a JSONL file; the browser reads `/api/live/stream` using Server-Sent Events with the JWT.
- SQL statements and API calls are captured automatically; service functions in `app/pipeline` and `app/services` are wrapped so Spark, SHAP and prediction steps appear too.
- Filters: Key steps (default), All, API, SQL, Upload, HDFS, Spark, AutoML, SHAP, Predict, System. Tabs: events and HDFS files. Controls: Pause, Clear, Close.

## 14. API reference

All routes except register, login and health need `Authorization: Bearer <token>`. Paths are listed from the project design; confirm exact paths and fields at `/docs`. [confirm]

| Router | Method and path | Purpose |
|---|---|---|
| health | `GET /api/health` | Liveness check |
| auth | `POST /api/auth/register` | Create account |
| auth | `POST /api/auth/login` | Get a JWT |
| datasets | `POST /api/datasets` (multipart file) | Upload and start preprocessing |
| datasets | `GET /api/datasets` | List your datasets |
| datasets | `GET /api/datasets/{id}` | One dataset |
| datasets | `GET /api/datasets/{id}/stats` | Column statistics |
| datasets | `GET /api/datasets/{id}/hdfs` | HDFS location and status of its files |
| training | `POST /api/training/...` | Start a run (dataset, target, engine) |
| training | `GET /api/training/...` | Runs and status; benchmark baseline vs optimized |
| models | `GET /api/models` | Trained models and leaderboard |
| predictions | `POST /api/predictions/...` | Predict from input values |
| predictions | `GET /api/predictions/...` | Prediction history |
| live | `GET /api/live/recent` | Recent events |
| live | `GET /api/live/stream` | SSE stream |
| live | `DELETE /api/live/events` | Clear events |
| live | `GET /api/live/hdfs` | HDFS status and file tree |

Example:

```bash
# register and log in
curl -s -X POST http://localhost:8000/api/auth/register -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"Secret123"}'
curl -s -X POST http://localhost:8000/api/auth/login -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=you@example.com&password=Secret123"   # [confirm form vs JSON]

# call a protected route
curl -s http://localhost:8000/api/datasets -H "Authorization: Bearer <token>"
```

## 15. Database schema

[confirm table and column names against `app/models.py`, and add the ER diagram.]

| Table | Main columns | Purpose |
|---|---|---|
| users | id, email, password hash, created at | Accounts |
| datasets | id, owner, name, file paths, status, row count, columns/profile | Uploaded data and its profile |
| training runs | id, dataset, target column, engine, status, timings | One training job |
| models | id, run, algorithm, metrics, best parameters, model path, selected flag | Leaderboard entries |
| predictions | id, model, inputs, output, created at | Prediction history |

Tables are created on startup by SQLAlchemy (`Base.metadata.create_all`).

## 16. Security notes

- Passwords are stored hashed, never in plain text.
- Every data route checks the JWT and that the dataset belongs to the user.
- Keep `.env` out of Git; rotate `SECRET_KEY` if it leaks.
- CORS is limited to the origins in `CORS_ORIGINS`.
- This is a student project: use HTTPS, a managed database and a stricter token policy before any real deployment.

## 17. Testing

Manual test checklist (record Pass or Fail in your report):

| Test | Expected |
|---|---|
| Register, log in | Token issued, pages load |
| Upload a CSV | Dataset becomes Ready; raw file in `/dataforge/raw` |
| Upload with Hadoop stopped | Upload still works; panel says HDFS offline |
| Text column with units | Converted to numbers by the cleaning step |
| Train XGBoost on text labels | No error, model listed |
| Benchmark baseline vs optimized | Both finish and the comparison appears |
| Predict with Fill random values | Result shown; target column is not in the form |
| Log out | Live stream stops |

Quick checks from the terminal:

```bash
curl -s http://localhost:8000/api/health
hdfs dfs -ls -R /dataforge
python3 -c "import app.main"        # imports without errors
```
If you add automated tests, put them in `tests/` and run `pytest`.

## 18. Git workflow for the team

```bash
git pull                                  # before you start
git checkout -b yourname-feature          # work on a branch
git add .
git commit -m "what changed"
git push -u origin yourname-feature       # then open a Pull Request on GitHub
```
Rules: never push `venv/`, `.env`, datasets or trained models (see `.gitignore`); review each other's Pull Requests; share secrets in a private chat, not in the repository.

## 19. Troubleshooting

| Symptom | Fix |
|---|---|
| "Load failed" on sign-up | Backend is not running or unreachable; check the uvicorn tab and `curl -s http://localhost:8000/api/health` |
| "HDFS offline" pill | Hadoop not running; start the NameNode and DataNode, then `curl -s "http://localhost:9870/webhdfs/v1/?op=GETFILESTATUS"` |
| `Permission denied` on HDFS upload | `export HDFS_USER=$(whoami)` and restart uvicorn |
| Live panel says "reconnecting" | Log in again or restart the backend |
| Prediction form shows the target column | Hard refresh; make sure you use the latest frontend file |
| XGBoost shows an error | Update `app/services/automl.py` to the latest version (text-label fix) |
| `ModuleNotFoundError` | Activate the venv and `pip install` the missing package |
| Port already in use | Stop the other process or run on another port |
| Anything breaks badly | Restore from your backup copy of the files |

## 20. Limitations and future work

**Limitations:** single-node Hadoop on one machine; jobs on background threads; Spark is used for profiling and cleaning, while model training runs with scikit-learn; modest speed-up on small datasets; outliers are not removed.

**Future work:** cloud deployment, Spark MLlib training on a cluster, Celery workers, outlier handling options, more explainers, automated tests and CI.

## 21. Team, license and credits

| Member | ID |
|---|---|
| Alen John Varghese | AM.SC.U4AID24002 |
| Gagan Chandran | AM.SC.U4AID24016 |
| S Sanjana | AM.SC.U4AID24036 |
| Sreelaya Vinod | AM.SC.U4AID24042 |
| Vaishnav K S | AM.SC.U4AID24048 |


Built with FastAPI, SQLAlchemy, PostgreSQL, Redis, Apache Hadoop, Apache Spark, scikit-learn, XGBoost, Optuna and SHAP.
