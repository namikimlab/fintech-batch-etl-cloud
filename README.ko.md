[🇺🇸 English](./README.md) | [🇰🇷 한국어](./README.ko.md)

# 💳 클라우드 통합 핀테크 배치 ETL
> Airflow, Spark, dbt, Redshift, S3 기반의 엔드투엔드 배치 데이터 파이프라인

[![Spark](https://img.shields.io/badge/Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)]()
[![Postgres](https://img.shields.io/badge/Postgres-336791?style=flat&logo=postgresql&logoColor=white)]()
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)]()
[![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)]()
[![Metabase](https://img.shields.io/badge/Metabase-509EE3?style=flat&logo=metabase&logoColor=white)]()
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)]()
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)]()
[![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=flat&logo=amazons3&logoColor=white)]()
[![Amazon Redshift](https://img.shields.io/badge/Amazon%20Redshift-8C4FFF?style=flat&logo=amazonredshift&logoColor=white)]()


![workflow](/screenshots/workflow.png)

# 개요
이 저장소는 금융 거래 데이터를 처리하기 위한 **프로덕션 스타일 Batch ETL 파이프라인**을 구현합니다.
파이프라인은 클라우드를 활용해 매일 수백만 건의 합성 거래 데이터를 **수집, 정제, 변환, 모델링**하는 과정을 보여줍니다.

**활용 사례**

> 매일 발생하는 신용카드 거래를 수집·검증한 후, Redshift 데이터 웨어하우스에 저장하여 **이상 거래 탐지, 신용 리스크 스코어링, 고객 세분화**와 같은 후속 분석을 지원합니다.

**주요 기능**

* **배치 적재**: Faker로 생성한 합성 거래 데이터를 S3 Bronze 계층에 저장
* **데이터 정제 및 변환**: Spark로 중복 제거, 정제, 파티셔닝 후 Silver 계층에 저장
* **웨어하우스 모델링**: dbt로 Redshift에 스테이징, 차원, 팩트, 마트 계층 구축
* **데이터 품질 검증**: Great Expectations(추가 예정)과 dbt 테스트
* **지연 도착 처리**: 최대 2일 늦게 도착하는 거래를 백필·중복 제거 로직으로 재처리
* **비용 및 성능 최적화**: Parquet/파티셔닝, Redshift sort/dist 키, 증분 업데이트
* **보안 모범 사례**: 최소 권한 IAM, S3 암호화, Secrets Manager를 통한 자격 증명 관리

# 아키텍처

## 파이프라인 구조

```
[Faker Generator] 
      │
      ▼
[S3 Bronze (Raw JSON/CSV)]
      │  (Spark로 정제, 중복 제거, 데이터 강화)
      ▼
[S3 Silver (Curated Parquet)]
      ├──► [S3 Gold]
      │
      └──► [Redshift Staging] → [Redshift Fact & Dim Tables] → [dbt Marts]
      
[Analytics / BI (Metabase)] → 마트 데이터 활용
```

## 구성 요소

![Airflow DAG](</screenshots/Screen Shot 2025-09-27 at 12.54.10 PM.png>)

* **Airflow** → 전체 파이프라인 실행 관리 (일일 ETL, 재시도, 백필)
* **Spark** → 원시 데이터를 정제·중복 제거·파티셔닝 후 Parquet 변환. 스키마 진화와 지연 데이터 처리 담당
* **AWS S3**
  * **Bronze**: 수집된 JSON/CSV (ingest_date 기준 파티션)
  * **Silver**: 정제된 Parquet (스키마 강제 적용 및 품질 검증 포함)
* **AWS Redshift** → 스테이징, 팩트/차원, 마트 테이블 저장소
* **dbt** → 마트 모델링 (RFM, LTV, 코호트 분석) 및 데이터 테스트
* **Great Expectations** → Silver 데이터를 로드하기 전 검증

## 데이터 흐름: Bronze → Silver → Gold

* **Bronze (Raw Zone)**

  * Faker 생성 거래 데이터가 직접 적재됨
  * 불변, schema-on-read, ingest date 기준 파티셔닝
  * 전체 원시 이력 보존 → 감사 및 재처리 가능

* **Silver (Curated Zone)**

  * Spark로 중복 제거, 스키마 강제 적용, 데이터 강화
  * Parquet 저장 (일자 + 업종 카테고리 기준 파티셔닝)
  * Great Expectations 검증 (null, enum, 범위 체크)

* **Gold (Business Zone)**

  * 스타 스키마 기반 모델링 (팩트 + 차원)
  * dbt를 통해 Redshift에 적재
  * BI 도구(Metabase)에서 고객 및 가맹점 분석에 활용

# 데이터 모델

## 개체
* **Cards** → 고객과 연결된 발급 카드
* **Merchants** → 업종 및 위치 정보를 포함한 가맹점 프로필
* **Transactions** → 신용카드 거래 이벤트 (핵심 팩트 테이블)

## 웨어하우스 계층

![dbt lineage](/screenshots/dbt_graph.png)

* **Staging (`stg_*`)** → S3 Silver에서 1:1 정제된 데이터
* **Dimensions (`dim_*`)** → 카드, 가맹점, 고객 차원
* **Facts (`fact_*`)** → 거래 단위 팩트 (중복 제거·강화 적용)
* **Marts (`mart_*`)** → 비즈니스 분석용 모델 (RFM, LTV, 코호트 분석)

# 저장소 구조

```bash
.
├── dags/                  # Airflow DAGs
├── data/                  # 입력 데이터
├── dbt/                   # dbt 프로젝트 (모델, 스키마, 프로필)
├── docker/                # Dockerfiles
├── great_expectations/    # Great Expectations 설정 및 스위트
├── jobs/                  # Spark 작업
├── logs/                  # Airflow/Spark/dbt 로그
├── scripts/               # 헬퍼 스크립트 (시드, 유틸리티)
├── .env                   # 환경 변수
├── docker-compose.yml     # 서비스 로컬 오케스트레이션
├── Makefile               # 빌드, 실행, 백필, 테스트 단축 명령어
├── README.md              # 프로젝트 문서
├── requirements.txt       # Python 의존성
```

# 설치 및 실행

* 모든 서비스는 **Docker 기반**으로 실행됩니다.

* 저장소 클론:

  ```bash
  git clone https://github.com/your-username/fintech-batch-etl.git
  cd fintech-batch-etl
  ```

* 로컬 서비스 시작:

  ```bash
  docker compose up -d
  ```

* Airflow 접속: [http://localhost:8080]

# 사용법

* **데이터 시딩** → `generate_transactions.py` 실행으로 합성 거래 생성
* **파이프라인 실행** → Airflow에서 `daily_batch_etl` DAG 실행
* **마트 빌드** → `dbt run` 실행으로 Redshift에 모델 생성
* **데이터 품질 검증** → dbt test + `run_great_expectations.py` 실행

# 모니터링 및 운영

![dbt test](/screenshots/dbt_test.png)

* **Airflow UI** → DAG 실행 현황, 재시도, 태스크 로그 확인
* **dbt docs / lineage** → 모델 의존성과 테스트 결과 확인

# 향후 개선 사항

현재 배치 ETL 파이프라인을 넘어 확장할 계획:

* **데이터 레이크하우스 강화**

  * S3 기반 Iceberg/Delta 테이블 도입 (ACID 및 upsert 지원)
  * Redshift Spectrum을 통한 외부 쿼리 검토

* **스트리밍 파이프라인**

  * Kafka → Spark Structured Streaming 추가로 준실시간 적재
  * 이상 거래 탐지 스트리밍 알림 데모

* **CI/CD 및 자동화**

  * GitHub Actions를 통한 dbt 자동 실행 및 테스트
  * Spark/Airflow 코드 린팅 및 단위 테스트

* **인프라 코드화**

  * Terraform으로 AWS 리소스(S3, Redshift, IAM) 관리
  * 환경 설정 파라미터화로 재현성 확보

* **모니터링 및 가시성**

  * OpenLineage를 dbt 모델에 확장 적용
  * 데이터 품질 오류 알림 (Slack/Email)

* **비용 최적화**

  * Redshift 서버리스 vs 클러스터 비용 비교
  * Redshift Spectrum, Glue + Athena를 통한 Ad-hoc 쿼리 활용

---

🪲 작성자: 김남희
[Portfolio](https://namikimlab.github.io/) | [GitHub](https://github.com/namikimlab) | [Blog](https://namixkim.com) | [LinkedIn](https://linkedin.com/in/namixkim)
