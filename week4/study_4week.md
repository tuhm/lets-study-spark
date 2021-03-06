### 맛보기!! 
#### 스파크의 집계 능력은 다양한 활용 사례와 가능성으로 보았을때 매우 정교하며 충분히 발달되어 있음!
#### 1) 집계 연산의 여러 함수 2)그룹화 방법 3)윈도우 함수 4)그룹화셋



### 집계연산  
#### 키나  그룹을  지정하고  하나  이상의  컬럼을  변환하는  방법 을  지정하는 집계  함수  사용
- 예.  특정 그룹의 평균값,  합산, 곱셈,  카운팅  
- 배열,  리스트  또는 맵  등의  복합  데이터  타입을  사용하여 집계  
#### 그룹화  데이터  타입   종류
1. SELECT구문의  집계  : 가장  간단한  형태의  그룹화    
: df.select(count('col')).show()
2. 'group by' 문 사용  :    하나  이상의 키  지정이 가능하며,  다른 집계  함수  사용  가능 
3. 윈도우(window)  :  하나 이상의  키  지정이 가능하며,  다른  집계  함수 사용  가능
: 함수의 입력으로 사용할  로우는  현재  로우와 어느정도  연관성이  있어야  함   
4. 그룹화 셋(grouping set) :  서로  다른  레벨의  값을  집계할 때  사용  
: SQL, DataFrame의 롤업, 큐브  
5. 롤업 (roll up) : 하나  이상의 키 지정이  가능하며, 다른  집계 함수  사용 가능
: 계층적으로  요약된 값을  구할  수  있음  
6. 큐브 (cube ) : 하나  이상의  키  지정이  가능하며, 다른  집계  함수 사용  가능  
: 모든 컬럼  조합에  대한 요약  값 계산 

#### NOTE :어떤 결과를  만들지  정확히 판단해야 함.  정확한 답을 얻기 위해서는  연산  비용이 크므로  정확도에  맞춰 근사치를  계산하는 것이  더  효율적임.  spark에서는 근사치  계산용 함수를  사용해 스파크 잡의 실행과  속를  개선할 수 있음 

``` 
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/all/*.csv")\
  .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")


``` 

- 적은  양의  데이터를  가진 파일이므로, 파티션 수를  줄이는  리파티셔닝을  수행하고  빠르게  접근 가능하도록  캐싱 

``` 
df.count() == 541909
``` 
- 트랜스포메이션이  아닌  액션 
- dataframe 캐싱  작업을  수행하는 용도로도  사용  


### 집계  함수 
####	집계  함수 정보  :  org.apache.spark.sql.functions 
#### count 
- count(*) 구문을  사용하면 null값을 가진  로우를 포함해  카운트
- 특정 컬럼 지정 시 null  값을  카운트 하지  않음 
``` 
from pyspark.sql.functions import count
df.select(count("StockCode")).show() # 541909


```


#### countDistinct 
- 전체 레코드 수가  아닌 고유의 레코드 수를 산출 
- 개별  컬럼을 처리하는 데  더  적합 

``` 
from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show() # 4070

```
#### approx_count_distinct 
- 대규모  데이터셋을  다룰 때  정확한 고유 개수가  무의미할 수  있음.  근사치만으로도 유의미하다면  해당  함수  사용  
- 최대 추정  오류율  파라미터 사용  :  큰  오류율일수록 더  빠르게  결과를  반환  
- 대규모  데이터셋을  사용할 때 훨씬 효율적임
``` 
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364

```
#### fisrt와  last 
- DataFrame의 첫번째  값이나 마지막  값을 얻을  때  사용하며, DataFrame의  값이  아닌 로우를  기반으로  동작 
``` 
from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()


```
#### min과  max 
- 최소값과  최댓값  추출
``` 
from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()


```
#### sum
- 특정  컬럼의  모든  값  합산
``` 
from pyspark.sql.functions import sum
df.select(sum("Quantity")).show() # 5176450

```
#### sumDistinct
- 특정   컬럼의  고윳값 합산
``` 
from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show() # 29310

```
#### avg
- 스파크의 avg나  mean  함수  사용하여  평균값  산출 
-	avgDistinct??  : 모든  고윳값의  평균  산출 가능
-	대부분의 집계함수는  고윳값을 사용해 집계를  수행하는 방식  지원
``` 
from pyspark.sql.functions import sum, count, avg, expr

df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()


```

#### 분산과  표준편차
- 분산,  표준편차 :  평균  주변에  데이터가  분포된  정도  측정 
- 분산  : 평균과의  차이를  제곱한  결과의  평균
- 표준편차  :  분산의  제곱근  
- 표본표준편차 뿐만  아니라  모표준편차  방식도 지원하기  때문에 다른 통계 방식이므로 구분해서 사용 
- variance 함수나 stddev 함수를 사용한다면 기본적으로 표본  표준 분산  공식을 이용함
- 모 표준 분산이나 모표준편차  방식을  사용하려면  var_pop함수 사용  
자료가 모집단인지  모집단을 대표하는 표본  집단인지에  따라 달라짐  
(표본  집단인  경우,  표본에  있는  자료값의 개수보다  작은 n-1로 나눔)
![image](https://user-images.githubusercontent.com/18010639/125221837-d3a2c900-e303-11eb-9822-38d55065d7ce.png)
``` 
from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()

```
#### 비대칭도와  첨도 
- 데이터의 변곡점을  측정하는  방법  
- 비대칭도  :  데이터  평균의  비대칭 정도  측정  
: 실숫값 확률변수의  확률분포  비대칭을 나타내는  지표로  '왜도'라고  함
- 첨도 :  데이터  끝 부분  측정
: 확률분포의  뾰족한 정도를  나타내는  척도  
- 비대칭도와 첨도는 확률  변수의  확률  분포로 데이터를 모델링할  때  특히  중요  
![image](https://user-images.githubusercontent.com/18010639/125224302-54fc5a80-e308-11eb-8cda-5d7bd8e492e3.png)
![image](https://user-images.githubusercontent.com/18010639/125224308-56c61e00-e308-11eb-9064-95322059f45c.png)

``` 
from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()

``` 

#### 공분산과  상관관계
- cov와 corr  함수를  사용해  공분산과  상관관계 계산 
- 공분산은  데이터  입력값에  따라  다른 범위를  가지며,  상관관계는  피어슨  상관계수를  측정하여  -1과  1사이의 값을 가짐
- var  함수처럼  표본공분산 방식이나 모공분산 방식으로  공분산  계산  가능 
``` 
from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()


```
####	복합  데이터  타입의 집계
- 스파크는  복합  데이터 타입을 사용해  집계 수행 가능  
- 특정  컬럼의 값을 리스트로 수집하거나  셋  데이터 타입으로 고윳값만 수집할 수  있음  
- 처리 파이프라인에서  다양한  프로그래밍 방식으로  다루거나  사용자  정의  함수에  활용  가능 
``` 
from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()

```
### 그룹화  
#### DataFrame  수준의  집계가  아닌,  데이터  그룹 기반의 집계  수행  
#### 단일  컬럼의  데이터를  그룹화  하고,  해당  그룹의  다른  여러 컬럼을  사용해서 계산하기 위해  카테고리형 데이터  사용 
#### 그룹화 단계
1.  하나 이상의  컬럼을 그룹화  ->  RelationalGroupedDataset 반환
2.  그룹에  집계  연산을 수행  ->  DataFrame 반환 
#### 그룹의  기준이 되는  컬럼을  여러개 지정 할  수  있음  

#### 표현식을  이용한  그룹화  
- 카운팅  메서드  대신 count  함수를  사용할  것을  추천  
- count함수를  select 구문에  표현식으로  지정하는  것보다  agg  메서드  사용하는  것을  추천  
1. 여러  집계  처리를  한번에  지정할  수  있음
2. 집계에 표현식을  사용할  수  있음
3. 트랜스포메이션이  완료된  컬럼에  alias  메서드를  사용할 수있음
``` 
from pyspark.sql.functions import count

df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()
```
#### 맵을  이용한  그룹화
-	컬럼을  키로, 수행할  집계함수의  문자열을  값으로  하는  맵타입을 사용해 트랜스포메이션을  정의할 수  있음 
- 수행할  집계함수를  한 줄로  작성하며  여러  컬럼명을  재사용할  수  있음
``` 
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
  .show()

#스칼라 버전 
import org.apache.spark.sql.functions.map
df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)

// 스칼라 버전
df.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop").show()

```
### 윈도우  함수  
#### 데이터의 특정  윈도우를  대상으로  고유  집계 연산 수행 
#### '윈도우'는  현재  데이터에  대한  참조를  사용해  정의  
#### 윈도우  명세는  함수에  전달될  로우를  결정  
#### group-by와의 차이점
- group-by는 모든  로우 레코드가  단일  그룹으로만  이동  
- 윈도우  함수는  프레임에  입력되는  모든  로우에  대해  결괏값을  계산  
- 프레임  :  로우 그룹  기반의  테이블 
![image](https://user-images.githubusercontent.com/18010639/125224314-59c10e80-e308-11eb-8623-e5e405066aca.png)
#### 3가지의  윈도우  함수  지원
1. 랭크  함수
2. 분석  함수
3. 집계  함수  
 
``` 
from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")


from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
  .partitionBy("CustomerId", "date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
``` 
``` 
from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)
``` 
``` 
from pyspark.sql.functions import col

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()
```

### 그룹화  셋  
#### 여러  컬럼을 집계  ->  group by 표현식
#### 여러  그룹에  걸쳐  집계  -> 그룹화 셋 
#### 그룹화셋은  여러  집계를 결합하는 저수준  기능 
- 그룹화 셋은 null 값에  따라 집계 수준이  달라짐. null값을 제거하지 않았다면 부정확한  결과를 얻게 되며, 이 규칙은 큐브,  롤업,  그리고 그룹화셋에 적용됨  
#### 그룹핑 키와  관계없이 총  수량의  합산결과를 추가하려면  group-by  구문을  사용해 처리하는  것은 불가능 -> 그룹화 셋 사용
- GROUPING  SET 구문에 집계  방식을 지정  
(여러개의 그룹을  하나로  묶는 것과  같음) 
- GROUPING SET은  SQL  에서만 사용 가능함
- DataFrame에서 동일한  연산을  수행하려면  roll up  메서드와 cube  메서드 사용  

``` 
SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
ORDER BY CustomerId DESC, stockCode DESC


SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode),())
ORDER BY CustomerId DESC, stockCode DESC
``` 


#### 롤업
##### group-by  스타일의  다양한 연산을  수행할  수  있는 다차원  집계  기능
##### 모든  날짜의  총합,  날짜별  총합, 날짜별  국가별  총합 모두  포함
##### null  값을  가진  로우에서  전체 날짜의  합계를 확인할  수 있음 
- 모두  null인  로우는  레코드에의 전체 합계를 나타냄
``` 
dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
  .orderBy("Date")
rolledUpDF.show()

```
#### 큐브
##### 롤업을  고차원적으로 사용할  수  있게 해주는 방법  
##### 요소를 계층적으로 다루는  대신  모든  차원에  대해 동일한 작업 수행 
##### 전체 기간에 대해 날짜와 국가별  결과를  얻을 수  있음 
- 전체  날짜와 모든  국가에  대한  합계
- 모든  국가의 날짜별  합계
- 날짜별  국가별  합계  
- 전체  날짜와  국가별 합계 
``` 
from pyspark.sql.functions import sum

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
  .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()


```
- 테이블에  있는  모든  정보를  빠르고 쉽게 조회할 수  있는 요약 정보  테이블을  만들  수 있음 


#### 그룹화  메타데이터
##### 큐브와  롤업 사용시 집계 수준에  따라 필터링하기 위해  집계 수준  조회
##### grouping_id는 결과  데이터셋의  집계  수준을  명시하는  컬럼  제공 
##### 예제
- 3: 가장 높은 계층의 집계 결과에서 나타남, customerId나 stockCode에 관계없이 총 수량 제공
- 2: 개별 재고 코드의 모든 집계 결과에서 나타남. customerId에 관계 없이 재고 코드별 총 수량 제공
- 1: 구매한 물품에 관계없이 customerId를 기반으로 총 수량 제공
- 0: customerId와 stockCode 별 조합에 따라 총 수량 제공
``` 
// 스칼라 버전
import org.apache.spark.sql.functions.{grouping_id, sum, expr}

dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
.orderBy(col("grouping_id()").desc)
.show()

```
#### 피벗
##### 로우를  컬럼으로  변환할 수  있음  
##### 카디널리티가 낮은 경우, 피벗을 사용하여 컬럼과 스키마를 확인하는 것이 좋음
- 카디널리티  :   중복도가  높으면  카디널리티가  낮다고 표현 
``` 
pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

```

### 사용자 정의  집계  함수 
#### 직접  제작한  함수나  비즈니스  규칙에  기반을  둔  자체  집계함수  정의하는  방법  
#### UDAF  사용해서  입력  데이터  그룹에  직접  개발한  연산을  수행할  수  있음  
#### 모든  그룹의  중간 결과를  단일 AggregationBuffer에 저장해 관리
#### UserDefineAggregateFunction을  상속
#### 현재 스칼라와  자바로만  사용할 수  있음 
#### 재정의 요소 
1. inputSchema: 진입 파라메터들
2. bufferSchema: 중간 결과들
3. dataType: 반환 데이터 타입
4. deterministic: 동일한 입력값에 대한 동일한 결과 반환 여부
5. initialize: inputSchema 초기화
6. update: 각 노드들이 내부 버퍼를 업데이트 하는 로직
7. merge: 노드 간 버퍼 병합 로직
8. evaluate: 최종 반환 결과값 생성 로직

``` 
// 스칼라 버전
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
class BoolAnd extends UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", BooleanType) :: Nil)
  def bufferSchema: StructType = StructType(
    StructField("result", BooleanType) :: Nil
  )
  def dataType: DataType = BooleanType
  def deterministic: Boolean = true
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = true
  }
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
  }
  def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}

// 스칼라 버전
val ba = new BoolAnd
spark.udf.register("booland", ba)
import org.apache.spark.sql.functions._
spark.range(1)
  .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
  .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
  .select(ba(col("t")), expr("booland(f)"))
  .show()

```
