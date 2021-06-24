
### 4장. 구조적 API 개요

#### 개요 끄적 끄적
- 구조적 API는 데이터 흐름을 정의하는 기본 추상화 개념 !!!!
- 구조적 API는 비정형 로그 파일부터 반정형 CSV 파일 매우 정형적인 파케이 파일까지 다양한 유형의 데이터를 처리 할 수 있음. 
- 배치와 스트리밍 처리에서 구조적 API 사용 가능.
- 구조적 API에 있는 분산 컬렉션 API
  1. Dataset : JVM기반, JAVA, Scala에서만 사용 가능
  2. DataFrame : Python, R 에서는 Dataframe을 사용. Dataframe은 최적화된 스파크 내부 포맷을 사용
  3. SQL 테이블과 View

**파케이파일 : 파케이는 데이터를 저장하는 방식(파일포멧) 중 하나. 파케이는 하둡에서 운영되는 언어, 프레임워크에 제한되지 않는 모든 프로젝트에서 사용가능한 컬럼 기반의 저장 포멧입니다. 파케이는 트위터에서 개발한 파일 포멧이며, 소스코드를 공개한 이후 아파치에서 관리

#### 이번장의 뽀인트는,
1. 타입형(typed)/비타입형(untyped) API의 개념과 차이점
2. 핵심용어
3. 스파크가 구조적 API의 데이터 흐름을 해석하고 클러스터에서 실행하는 방식

#### 스파크의 구조적 데이터 타입 개요

- 스피크는 실행 계획 수립과 처리에 사용하는 "자체 데이터 타입 정보"를 가지고 있는 키탈리스트 엔진을 사용.
- 파이썬이나 R을 이용해 스파크의 구조적 API를 사용하더리도 대부분의 연산은 피이썬이나 R의 data 타입이 아닌 스파크의 data 타입을 시용.
- 파이썬 코드를 수행 하더라도, 스피크에서 덧셈 연산이 수행되는 이유는 스파크가 지원하는 언어를 이용해 작성된 표현식을 카탈리스트 엔진에서 스파크의 데이터 타입으로 "변환"해 명령을 처리하기 때문

#### Dataframe VS Dataset
- Dataframe과 Dataset은 잘 정의된 로우와 컬럼을 가지는 분산 테이블 형태의 컬렉션
- 컬렉션의 모든 로우는 같은 데이터 타입을 가져야 한다
- DataFrame '비타입형', DataFrame에도 데이터 타입이 있습니다. 히지만 스키마에 명시된 데이터 타입의 일치 여부를 '런타임'이 되어서야 확인합니다. 
- Dataset은 '타입형', 스키마에 명시된 데이터 타입의 일치 여부를 '컴파일'에 확인. Dataset은 JVM기반의 언어인 스칼라와 자바에서만 지원.
- 스파크의 DataFrame은 "Row 타입으로 구성된 Dataset"입니다. Row 타입은 스파크가 사용하는 "연산에 최적회된 인메모리 포맷"의 내부적인 표현 방식. 
- Row 타입을 사용하게 되면 자체 데이터 포멧을 사용하기 때문에 매우 효율적인 연산이 가능
- 지금 기억해야 할 것은 dataframe을 사용하면 스파크의 최적회된 내부 포맷을 시용할 수 있디는 사실. 스파크의 최적회된 내부 포맷을 시용하면 스파크가 지원하는 어떤 언어 API를 시용하더리도 동일한 효과와 효율성을 얻을 수 있습니다.

- 컬럼의 개념 : 정수,문자열 - 단순 데이터 타입 / 배열, 맵 - 복합 데이터 타입 (5장에서 자세히 다룰 예정)
- 로우의 개념 : 데이터 레코드
- 특정 Data type의 컬럼을 초기화 하는 방법
// from pyspark.sql.types import *
// b = ByteType()

#### 구조적 API 실행 과정
- Spark Code가 클러스터에서 실제 처리되는 과정

	1. DataFrame/Dataset/SQL을 이용해 코드를 작성
	2. 정상적인 코드라면 스파크가 논리적 실행 계획으로 변환
*논리적 실행 계획 : 추상적인 트랜스포메이션만! 실행계획을 검증하지 않은 상태! 사용자의 다양한 표현식을 최적화된 버전으로 변환(검증 전 논리적 실행 계획), 코드유형, 테이블, 컬럼 존재 data check
*사용자 코드 -> 검증전 논리적 실행계획 -> 카탈로그 분석 -> 검증된 논리적 실행계획 -> 논리적 최적화 -> 최적화된 논리적 실행계획
	3. 카탈리스트 옵티마이저가 수행. Spark는 논리적 실행 계획을 물리적 실행 계획으로 변환하며 그 과정에서 추가적인 최적화를 할 수 있는지 확인
*카탈리스트 옵티마이저 : 조건절 푸쉬다운이나 선택절 구문을 이용하여 논리적 실행계획을 최적화 시키는 규칙모음
*물리적 실행 계획 : 논리적 실행계획을 클러스터 환경에서 실행하는 방법. 비용을 계산해서 최적의 물리적 계획 선택하는데 예를들어 물리적 속성을 고려해 지정된 조인 연산 수행에 필요한 비용을 계산하고 비교. DataFrame, Data set, SQL로 정의된 쿼리를 RDD 트랜스포메이션으로 컴파일한다. 스파크를 컴파일러라고 불리우기도 한다
	4. 스파크는 클러스터에서 물리적 실행 계획(RDD 대상)을 실행
        *실행 : 저수준 프로그래밍 인터페이스인 RDD를 대싱으로 모든 코드를 실행


### 5장. 구조적 API 기본 연산 

- 이번장의 뽀인뜨!!! DataFrame 기본 기능을 다루는데 뽀인뜨!!! 레코드, 컬럼, 스키마, 파티셔닝, 파티셔닝 스키마 (파티션을 배치히는 방법을 정의)

#### 스키마 : 스키마는 분산 컬렉션에 저장할 데이터 타입을 정의하는 방법, Data Frame에서 컬럼명과 데이터 타입을 정의

	1.스키마 확인 
	print(spark.read.format("json").load("/user/fp10186/2015-summary.csv").schema)
	2.스키마 생성 : 직접생성, 데이터 소스에서 얻기. 여러개의 StructField 타입 필드로 구성된 StructType 객체.
	from pyspark.sql.types import StructField, StructType, StringType, LongType
	*StructField (이름 data type, null이 가능한지)
	
	myManualSchema = StructType([
	StructField("DEST_COUNTRY_NAME", StringType(), True),
	StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
	StructField("count", LongType(), False, metadata={"hello":"world"})])
	df = spark.read.format("json").schema(myManualSchema).load("/user/fp10186/2015-summary.csv")

	ex. metadata 예시 - 데이터 위치, 파티션 정보, Description 보유
	customSchema = StructType([
  	StructField("cat_id", IntegerType(), True, {'description': "Unique id, primary key"}),
  	StructField("cat_title", StringType(), True, {'description': "Name of the category, with underscores"}) ])

#### 컬럼 : 사용자는 "표현식"으로 DataFrame의 컬럼을 선택, 조작, 제거 가능. 컬럼 내용을 수정하려면 반드시 DataFrame의 스피크 트랜스포메이션을 사용.

	1.컬럼 생성 : col
	from pyspark.sql.functions import col, column
	col("someColumnName")
	print(column("someColumnName"))
	
	2.표현식 : DatatFrame레코드의 여러값에 대한 transformation 집합을 의미. expr("someCol")은 col("someCol")과 동일. 
	단일값을 만들기 위해 다양한 표현식을 각 레코드에 적용하는 함수. 표현식은 "expr" 함수로 가장 간단히 사용할 수 있습니다.
	표현식을 사용해서 컬럼을 select, control, remove 할 수 있다.
	**핵심내용 : 컬럼은 단지 '표현식'일 뿐. 컬럼과 컬럼의 트랜스포메이션은 파싱된 표현식과 동일한 논리적 실행 계획으로 컴파일됩니댜
	
#### 레코드와 로우 : 스파크에서 DataFrame의 각 로우는 하나의 레코드. 동일한것. 여기서는 로우 사용

	1.로우에 접근하기 : print(myRow[2]) 로우 컬럼 추가, 제거, 로우를 컬럼으로 변환하거나 그 반대로 변환, 컬럼 값을 기준으로 로우 순서 변경
	import org.apache.spark.sql.Row
	val myRow = Row("Hello, null, 1, false)			
	// 데이터에 접근하는 방법
	myRow(0)  // Any type
	myRow(0).asInstanceOf[String]  // String type
	myRow.getString(0)  // String type
	myRow.getInt(2)  // Int type
	 
#### DataFrame 생성하기

	1.data 소스 이용 : 
	df = spark.read.format("csv").load("/user/fp10186/2015-summary.csv")
	df.createOrReplaceTempView("dfTable")
		
	2.Row 객체를 가진 seq타입을 직접 변환해 DataFrame을 생성
	from pyspark.sql import Row
	from pyspark.sql.types import StructField, StructType, StringType, LongType
	myManualSchema = StructType([StructField("some", StringType(), True),StructField("col", StringType(), True),StructField("names", LongType(), False)])
	myRow = Row("Hello", None, 1)
	myDf = spark.createDataFrame([myRow], myManualSchema)

#### select 와 selectExpr (select + expr) 
- DataFrame에서 SQL 사용하기
- selectExpr메서드는 새로운 DataFrame을 생성하는 복집한 표현식을 간단하게 만드는 도구
- 사실 모든 유효한 비집계형 SQL 구문을 지정할 수 있습니다. 단, 컬럼을 식별 할수 있어야 한다.
- Select 와 selectExpr 차이점 : Spark SQL function selectExpr() is similar to select(), the difference being it takes a set of SQL expressions in a string to execute. This gives an ability to run SQL like expressions without creating a temporary table and views.
- selectExpr() just has one signature that takes SQL expression in a String and returns a new DataFrame. Note like select() it doesn’t have a signature to take Column type and Dataset return type.

		df.select("DEST_COUNTRY_NAME").show(2)
		df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)		
		from pyspark.sql.functions import expr, col, column
		df.select(expr("DEST_COUNTRY_NAME"),col("DEST_COUNTRY_NAME"),column("DEST_COUNTRY_NAME"))
		df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
		df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))
		df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)	
		df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

#### 스파크 데이터 타입 변환 : 명시적인 값을 스파크에 전달해야 할 때, 리터럴(literal)을 사용. 리터럴은 프로그래밍언어의 Return값을 스파크가 이해 할 수 있는 값으로 변환합니다.
	from pyspark.sql.functions import lit
	df.select(expr("*"), lit(1).alias("One")).show(2)
	
	EX.
	df.withColumn("spanish_hi", lit("hola")).show()
	
#### 컬럼추가하기 : 공식적인 방법은 DataFrame withColumn 메서드를 시용. 컬럼명 변경도 가능.
	df.withColumn("numberOne", lit(1)).show(2)
	df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)

#### 컬럼명 변경하기 : withColumnRenamed 사용
	df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns  (이전, 변경)
	*공백이나 하이픈은 컬럼며에 사용이 불가합니다. 사용하려면 백틱 (') 이용
	*대소문자 구분하지 x
	
#### 컬럼제거 : drop
	df.drop(''0RI61NCOUNTRY빼AME',).columns
	dfWithLongCoIName.drop(,'0RIGIN COUNTRY NAME.', '.DE5T COUNTRY NAME'')
	
#### 컬럼 데이터 타입 변경 : cast 메서드 사용
	df.withColumn( 'count' , col('count').cast ( "string" ) )   (int -> stirng)
	import pyspark.sql.functions as f
	for column in null_columns:
    		df = df.withColumn(column, f.col(column).cast("string"))

#### 로우 필터링하기 : 참과 거짓 판별하는 표현식을 만들어 사용. where 메서드, filter 메서드 사용
	df.filter(col("count") < 2).5how(2)
	df.Where("count < 2").show(2)

#### 고유한 로우 얻기 : 중복제거 된 로우 얻기. distinct 이용.
	df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
	df.select("ORIGIN_COUNTRY_NAME").distinct().count()
	
#### 무작위 샘플 만들기 : sample 메서드 이용
	seed = 5
	###복원추출/비복원추출
	withReplacement = False
	###표본비율
	fraction = 0.5
	print(df.sample(withReplacement, fraction, seed).count())

#### 임의 분할하기 : data split 할 때 사용
	dataFrames = df.randomSplit([0.25, 0.75], seed)
        dataFrames[0].count() > dataFrames[1].count() # False
	
#### 로우 합치기와 추가하기 : 
- dataframe은 기존에 있는 곳에서 변경은 불가능하기 때문에 추가하고자하는 df와 기존 df를 합쳐야 한다.
- 이때 통합하려는 2개의 df는 반드시 동일한 스키마와 컬럼수를 가지고 있어야 한다. 
- DataFrame을 뷰로 만들거나 테이블로 등록하면 DataFrame 변경 작업과 관계없이 동적으로 침조할 수 있습니다.
	
		from pyspark.sql import Row
		schema = df.schema
		newRows = [Row("New Country", "Other Country", 5L),Row("New Country 2", "Other Country 3", 1L)]
		parallelizedRows = spark.sparkContext.parallelize(newRows)
		newDF = spark.createDataFrame(parallelizedRows, schema)
		df.union(newDF).where("count = 1").where(col("ORIGIN_COUNTRY_NAME") != "United States").show()

#### 로우 정렬하기  : sort, order by 메서드 이용. 기본은 오름차순
	df.sort("count").show(5)
	df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
	df.orderBy(expr("count desc")).show(2) ==> 이것은 expr(count as desc) 로 인식 해서 count 라는 컬럼이 오름차순으로 자동 정렬
	-> 제대로 수행하려면 
	(1) df.orderBy(col("count").desc()).show(3)
	(2) import pyspark.sql.functions as f
	f.expr("count desc")

	*파티션 정렬 : sortWithinPartitions
	spark.read.format("json").load("/data/flight-data/json/*-summary.json").sortWithinPartitions("count")
	
#### 로우수 제한 : limit 메서드
	df.limit(5).show()
	df.orderBy(expr("count desc")).limit(6).show()
 
#### Repartitoion 과 calesce : Repartition은 무조건 전체 데이터를 셔플

	df.rdd.getNumPartitions() # 1
	df.repartition(5)
	df.repartition(col("DEST_COUNTRY_NAME"))  #자주 필터링 되는 컬럼을 기준으로 파티션 재분배
	df.repartition(5, col("DEST_COUNTRY_NAME")) #선택적 파티션 수 지정 
	df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2) # Coalesce 메서드는 전체 데이터를 셔플하지 않고 파티션을 병합하려는 경우에 시용합니다 -> 다음은 목적지를 기준으로 셔플을 수행해 5개의 파티션으로 니누고, 전체 데이터를 셔플 없이 병합

#### 드라이버로 로우 데이터수집하기 : 스파크는 드라이버에서 클러스터 상태를 유지. 로컬 환경에서 데이터를 다루려면 드라이버로 데이터를 수집

	1.collect : 전체 DataFrame의 모든 데이터를 수집하기
	2.take : 상위 N개의 로우를 빈환합니다
	3.show : 여러 로우를 보기 좋게 출력
	4.toLocallterator : Iterator(반복자)로 모든 파티션의 데이터를 드라이버에 전달함. toLocallterator 메서드를 사용해 데이터셋의 파티션을 차례로 반복 처리

		collectDF = df.limit(10)
		collectDF.take(5) # take works with an Integer count
		collectDF.show() # this prints it out nicely
		collectDF.show(5, False)
		collectDF.collect()

