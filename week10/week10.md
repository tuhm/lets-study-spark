# 17장. 스파크 배포 환경
- 스파크 애플리케이션 실행에 필요한 인프라 구조
+ 클러스터 배포 시 선택사항
+ 스파크가 지원하는 클러스터 매니저
+ 배포 시 고려사항과 배포 환경 설정
- 스파크 지원 클러스터 매니저는 유사하지만, 다양한 클러스터 구성 방법이 있음 -> 근본적인 차이점에 대해 설명
- 스파크가 지원하는 클러스터 매니저
+ 스탠드얼론 모드
	+ 스파크에 패키지 되어있는 클러스터 관리자, 가장 간단함
	+ 동적으로 자원 사용량 조절이 되지 않음, 여러 애플리케이션 동시 사용 X
+ 하둡 YARN
	+ 하둡에서 사용할 수 있는 클러스터 관리자, 장시간 실행되는 배치 작업에 최적화
	+ 자원 초과 사용할 경우 얀이 터져요!
+ 아파치 메소스(Mesos)
	+ 짧게 실행되고 없어지는 워크로드에 적합
	+ cluster configuration이 Yarn보다 유연하지 못함
[Remarks](https://blog.naver.com/chemicalatom/220886863538)

## 17.1 스파크 애플리케이션 실행을 위한 클러스터 환경
+ 설치형 클러스터
+ 공개 클라우드

### 17.1.1 설치형 클러스터 배포 환경
- 자체 데이터센터를 운영하는 조직에 적합 (Zeppelin)
- 설치형 클러스터 환경은 사용 중인 하드웨어를 완전히 제어할 수 있음 -> 워크로드의 성능을 최적화
+ 설치형 클러스터의 문제점
	+ 크기가 제한적, 자원에 따라 적당한 클러스터의 크기를 설정해야 함
	+ HDFS나 분산 키-값 저장소 같은 자체 저장소 시스템을 운영해야 함
	+ 상황에 따라 지리적 복제 및 재해 복구 체계 구축
+ 문제점 1 - 자원 활용 문제 해결 방법은 클러스터 매니저 사용
	+ 클러스터 매니저 사용 시 다수의 스파크 애플리케이션 실행 가능
	+ 애플리케이션 자원 동적 재할당 가능
	+ 동시에 여러 애플리케이션 실행 가능
		+ 동적 자원 공유 : YARN, 메소스 > 스탠드얼론 모드
	+ 자원 공유를 제어하는 것이 가장 큰 차이점
		+ 공개 클라우드는 애플리케이션에 맞는 규모의 클러스터를 할당해줌
+ 문제점 2, 3 - 저장소 시스템 사용으로 해결
	+ 하둡의 HDFS (분산 파일 시스템), 아파치 카산드라 (키-값 저장소 시스템) 주로 사용

### 17.1.2. 클라우드 배포 환경
- 설치형 클러스터 -> 클라우드 환경으로 진화
+ 클라우드 배포 환경의 장점 - 유연성
	+ 자원을 탄력적으로 늘이고 줄이는 것이 가능
		+ 막 써도 사용한 만큼 비용 지불하면 끝 ^^
		+ 딥러닝이 필요할 때만 GPU 인스턴스 할당받아 쓸 수 있음 (Jutopia)
	+ 공개 클라우드 환경은 비용이 저렴 + 지리적 복제 기능 지원함 -> 대규모 데이터 쉽게 관리
	+ 환경 관리를 따로 하지 않아도 됨
- AWS, Microsoft Azure, GCP
- 고정된 크기의 클러스터와 파일 시스템은 클라우드와 같은 유연한 시스템과 맞지 않음
	- 파일 시스템도 Amazon - S3, Azure Blob, Google Cloud와 같이 분리된 저장소 시스템을 사용해야 함
	- 스파크 워크로드마다 별도의 클러스터를 동적으로 할당해야 함
- 데이터브릭스에서 무료로 클라우드 배포 환경을 사용할 수 있음

## 17.2 클러스터 매니저

### 17.2.1 스탠드얼론 모드
- 스파크 워크로드용으로 특별히 제작된 경량화 플랫폼
- 간단하고 빠르게 환경을 구축, 스파크 어플리케이션만 사용하는 시스템에 적합

#### 스탠드얼론 클러스터 시작하기
[Remarks](https://opennote46.tistory.com/238)
마스터 프로세스 실행
``` C
$SPARK_HOME/sbin/start-master.sh 
```
- spark://HOST:PORT를 통해 마스터 웹 UI 확인 가능
워커노드 프로세스 실행 (슬레이브 구동)
``` C
$SPARK_HOME/sbin/start-slave.sh
```
- 시작 스크립트에 두 명령어를 설정하여 스탠드얼론 클러스터 시작 자동화 가능
- conf/slaves 파일을 생성하여, 스파크 워커로 사용할 모든 머신의 호스트명을 기록해야 함 -> 안할 경우, 모두 로컬에서 실행됨
```C
$SPARK_HOME/sbin/start-all.sh # 마스터 + 모든 슬레이브 구동
$SPARK_HOME/sbin/stop-master.sh # 마스터 중지
$SPARK_HOME/sbin/stop-slaves.sh # 슬레이브 중지
$SPARK_HOME/sbin/stop-master.sh # 마스터 + 슬레이브 중지
```
- 스탠드얼론 클러스터 설정은 [여기](http://bit.ly/2xg4FEN)
	- 포트, 메모리, 자바옵션, Core, Class path 설정 가능
	- SPARK_WORKER_MEMORY, SPARK_WORKER_CORES

### 17.2.2 YARN
- 잡 스케줄링과 클러스터 자원 관리용 프레임워크
- 하둡과 스파크는 독립적임!
- 스탠드얼론 모드에 비해 더 많은 기능 사용 가능

#### 어플리케이션 제출하기
마스터 인수 값을 yarn으로 지정해야 함 --master yarn
- yarn cluster mode
	- 스파크 드라이버 프로세스를 관리
- yarn client mode
	- 마스터 노드를 관리하지 않고 애플리케이션에 익스큐터 자원을 배분
+ yarn 속성을 이용하여 우선순위 큐와 보안에 필요한 keytab 파일 제어 가능

### 17.2.3 YARN 환경의 스파크 어플리케이션 설정하기

#### 하둡 설정
/etc/hadoop/conf 하위에 설정 파일 존재
- hdfs-site.xml : HDFS 클라이언트 동작 방식 설정
- core-site.xml : 기본 파일 시스템의 이름을 설정

#### YARN 속성
- resource, memory, core, waitTime, jar file, queue 설정 가능 [여기](http://spark.apache.org/docs/latest/running-on-yarn.html#configuration)
	- spark.yarn.queue
	- spark.yarn.keytab
	- spark.yarn.am.cores

### 17.2.4 메소스에서 스파크 실행하기
- CPU, 메모리, 저장소, 다른 연산 자원을 머신에서 추상화
- 탄력적 분산 시스템 구성 -> 효과적으로 실행
- 웹 어플리케이션이나 다른 자원 인터페이스 등 오래 실행되는 어플리케이션까지 관리할 수 있는 데이터센터 규모의 클러스터 매니저
- 가장 무거워서 환경이 잘 구축되어 있는 경우 사용
+ 두 가지 메소스 모드
	+ fine-grained mode
	+ coarse-grained mode : 현재는 이것만 지원
		+ 스파크 익스큐터를 단일 메소스 태스크로 실행
		+ spark.executor.memory, spark.executor.cores 등으로 크기 조절

#### 어플리케이션 제출하기
- cluster 모드 방식이 가장 좋음
``` C
export MESOS_NATIVE_JAVA_LIBRARY=<libmesos.so 파일의 경로>
export SPARK_EXECUTOR_URI=<업로드한 spark-2.2.0.tar.gz 파일의 URL>
```
```scala
import org.apache.spark.5ql.5parkSession
val Spark = SparkSession.builder
.master("mesos://HOST:5050")
.appName("my app")
.config("spark.executor.uri", "<업로드한 spark-2.2.0.tar.gz 파일의 URL>") .getOrCreate()
```

#### 메소스 설정하기
- core, volumn, memoryOverhead [여기](http://bit.ly/2DPmLTf)
	- spark.mesos.extra.cores
	- spark.mesos.mesosExecutor.cores

### 17.2.5 보안 관련 설정
- 신뢰도가 낮은 환경에서 안전하게 실행될 수 있도록 저수준 API 기능 제공
+ 통신 방식과 관련이 깊음
	+ 인증, 네트워크 구간 암호화
	+ TLS, SSL 설정

### 17.2.6 클러스터 네트워크 설정
- proxy 사용 등 네트워크 설정
	- 주소값, 대기 시간, Retry 횟수, Timeout

### 17.2.7 어플리케이션 스케줄링
- 독립적인 익스큐터 프로세스 실행
	- 클러스터 매니저가 스파크 어플리케이션 전체에 대한 스케줄링
- 여러 개의 잡을 다른 스레드가 제출한 경우 동시에 실행 가능
	- 페어 스케줄러 기능을 제공함 : 공정하게 실행 중인 모든 잡에 자원을 동적 분배
	- 가장 간단한 방법이 자원을 고정된 크기로 나눔 -> 어플리케이션 종료 전까지 해당 자원 점유

