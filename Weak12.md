### 6부 요약
#### 주요 Code 정리

  1. Train/Test Split
<pre>
<code>
  train, test = preparedDF.randomSplit([0.7, 0.3])
</code>
</pre>

2. Logistic 
<pre>
<code>
  from pyspark.ml.classification import LogisticRegression
  lr = LogisticRegression(labelCol="label",featuresCol="features")
  print lr.explainParams() *hyperparameter*
  fittedLR = lr.fit(train)
  fittedLR.transform(train).select(''label", ''prediction'').show() *predict value*
</code>
</pre>

  3. Pipeline
<pre>
<code>
from pyspark.ml.tuning import TrainValidationSplit
tvs = TrainValidationSplit()\
  .setTrainRatio(0.75)\  *Split*
  .setEstimatorParamMaps(params)\ *Pre Processing*
  .setEstimator(pipeline)\   *pipeline = Pipeline().setStages(stages)*
  .setEvaluator(evaluator) *AUROC*
tvsFitted = tvs.fit(train)
</code>
</pre>

## Chapter 25. 데이터 전처리 및 엔지니어링

#### 25.1 사용 목적에 따라 모델 서식 지정하기
- 다양한 형태의 데이터를 확보하는 가장 좋은 방법 -> 변환자를 사용
- 변환자는 인수로 Dataframe을 받고 새로운 Dataframe 반환
- sales.cache ( ) #자주 사용하는 Data의 경우 메모리에서 효율적으로 읽을수 있도록 캐시
- MLlib의 경우 **Null이 존재하면 작동을 하지 않은 경우가 많으므로 디버깅 할때 가장 먼저 확인**
- 변환자(transformer): 한 데이터셋을 다른 데이터셋으로 변환. 스파크 ML의 머신러닝 모델도 데이터셋에 예측 결과를 더하는 변환 작업을 수행하므로 변환자에 해당한다. 변환자의 핵심 메서드인 **transform**은 DataFrame을 필수 인수로 받고, 선택 인수로 매개변수 집합을 받는다.
- 추정자(estimator): 주어진 데이터셋을 **학습해 변환자를 생성**. 추정자의 결과는 학습된 선형 회귀 모델(변환자)이라고 할 수 있다. 추정자의 핵심 메서드인 **fit**은 DataFrame을 필수 인수로 받고, 선택 인수로 매개변수 집합을 받는다.

#### 25.4 전처리 추정자 
- 전처리를 위한 또 다른 도구 
- 수행하려는 변환이 입력 컬럼에 대한 데이터 또는 정보로 초기화 되어야 할 때 필요
- 추정자는 단순 변환을 위해 맹목적으로 적용하는 *일반 변환자 유형* 과 데이터에 따라 변환을 수행하는 *추정자 유형*

#### 25.4 고수준 변환자
##### 25.4.1 RFormula
- 일반적인 형태의 데이터에 사용할 수 있는 가장 간편한 변환자
- 숫자 컬럼은 Double 타입 변환, 원-핫 인코딩은 되지 않음
- Label 컬럼이 String 타입인 경우 먼저 StringIndexer를 사용해서 Double 타입으로 변환
<pre>
<code>
from pyspark.ml.feature import RFormula
supervised = RFormula(formula="lab ~ . + color:value1 + color:value2")
supervised.fit(simpleDF).transform(simpleDF).show()
</code>
</pre>
- 참고
  - '~' 함수에서 타깃과 항을 분리
  - '+' 연결 기호, '+0' 은 절편 제거
  - '-' 삭제 기호 '-1' 절편제거
  - ':' 상호작용 (수치평 값이나 이진화된 범주값에 대한 곱셈)
  - '.' 타깃/종속변수를 제외한 모든 컬럼

##### 25.4.2 SQL 변환자 
- SQL Transformer 사용
- SQL 에서 사용하는 모든 select 문은 유효함. 단, 테이블 이름 대신 THIS 키워드 사용
<pre>
<code>
from pyspark.ml.feature import SQLTransformer
basicTransformation = SQLTransformer()\
  .setStatement("""
    SELECT sum(Quantity), count(*), CustomerID
    FROM __THIS__
    GROUP BY CustomerID
  """)
basicTransformation.transform(sales).show()
</code>
</pre>

##### 25.4.3 벡터 조합기
- VectorAssempler는 사용자가 생성하는 거의 모든 단일 파이프라인에서 사용하게될 도구
- 모든 특징을 하나의 큰 벡터로 연결 하여 추정자에 전달하는 기능 제공
- 파이프 라인의 마지막 단계에서 사용
<pre>
<code>
from pyspark.ml.feature import VectorAssembler
va = VectorAssembler().setInputCols(["int1", "int2", "int3"])
va.transform(fakeIntDF).show()
-----------------> 아웃풋 [1, 2, 3] 
</code>
</pre>

#### 25.5 연속형 특징 처리하기
- 버켓팅 : 연속형 특징을 범주형으로 변환
- 스케일링 및 정규화 
- 이러한 변환자 사용을 위해서는 Data type이 Double Type이어야함
- contDF = spark.range(20).selectExpr("cast(id as double)") *형변환*

##### 25.5.1 버켓팅
- Bucketizer 사용 (그룹화, Binning)
- 분할값 (기준치)은 df의 최솟값 보다 작아야하며, 최댓값 보다 커야 하며, 최소 3개 이상의 값을 지정해서 두개 이상의 버켓을 만들어야 함
<pre>
<code>
from pyspark.ml.feature import Bucketizer
bucketBorders = [-1.0, 5.0, 10.0, 250.0, 600.0]
bucketer = Bucketizer().setSplits(bucketBorders).setInputCol("id")
bucketer.transform(contDF).show()
-> 0 : -1 <= x < 5 
-> 1: 5 <= x < 10
-> 2: 10 <= x < 250
</code>
</pre>
- 백분위수로도 분할 가능. QunatileDixcretizer 로 수행
<pre>
<code>
from pyspark.ml.feature import QuantileDiscretizer
bucketer = QuantileDiscretizer().setNumBuckets(5).setInputCol("id").setOutputCol("result")
fittedBucketer = bucketer.fit(contDF)
fittedBucketer.transform(contDF).show()
</code>
</pre>

##### 25.5.2 스케일링과 정규화
- MLlib에서는 항상 Vector 타입의 컬럼에서 이 작업을 수행

##### 25.5.3 
[StandardScaler]
- 평균이 0 이고 표준편차가 1인 분포를 갖도록 데이터를 표준화
- withStd 플래그는 Data를 표준편차가 1이 되도록 스케일링 하는 것
- withMean플래그는 스케일링 하기전에 데이터를 센터링(centering)
<pre>
<code>
from pyspark.ml.feature import StandardScaler
sScaler = StandardScaler().setInputCol("features")
sScaler.fit(scaleDF).transform(scaleDF).show()
</code>
</pre>

[MinMaxScaler]
- 최소값을 0으로 지정하고 최댓값을 1로 지정. 비례값으로 스케일링
<pre>
<code>
from pyspark.ml.feature import MinMaxScaler
minMax = MinMaxScaler().setMin(5).setMax(10).setInputCol("features")
fittedminMax = minMax.fit(scaleDF)
fittedminMax.transform(scaleDF).show()
</code>
</pre>

[MaxAbsScaler]
- 최대 절댓값 스케일러는 각 값을 해당 컬럼의 최대 절댓값으로 나눠서 데이터의 범위를 조정. 
- 모든 값은 -1 ~ 1 사이로 조정
<pre>
<code>
from pyspark.ml.feature import MaxAbsScaler
maScaler = MaxAbsScaler().setInputCol("features")
fittedmaScaler = maScaler.fit(scaleDF)
fittedmaScaler.transform(scaleDF).show()
</code>
</pre>

[Elementwise Product]
- 벡터의 각 값을 임의의 값으로 조정 (행렬의 곱으로 조정)
<pre>
<code>
from pyspark.ml.feature import ElementwiseProduct
from pyspark.ml.linalg import Vectors
scaleUpVec = Vectors.dense(10.0, 15.0, 20.0)
scalingUp = ElementwiseProduct()\
  .setScalingVec(scaleUpVec)\
  .setInputCol("features")
scalingUp.transform(scaleDF).show()
</code>
</pre>

[Normalizer]
- 여러가지 표준 중 하나를 사용하여 다차원 벡터를 스케일링
- 위 방안들은 각 통계치를 이용하나, Normalizer는 각 로우마다 정규화 -> 그 거리의 기준을 'p'를 이용해 지정
- 파라미터 'p' 로 지정 (맨해튼 표준 p '1', 유클리드 표준 p '2')
<pre>
<code>
from pyspark.ml.feature import Normalizer
manhattanDistance = Normalizer().setP(1).setInputCol("features")
manhattanDistance.transform(scaleDF).show()
</code>
</pre>

#### 25.6 범주형 특징 처리하기

##### 25.6.1 String Indexer
- 문자열을 다른 숫자 ID에 매핑 (label encoding)
- df에 첨부된 메타 데이터를 생성하여 나중에 각 색인값 에서 입력 값을 다시 가져올 수 있음
<pre>
<code>
from pyspark.ml.feature import StringIndexer
lblIndxr = StringIndexer().setInputCol("lab").setOutputCol("labelInd")
idxRes = lblIndxr.fit(simpleDF).transform(simpleDF)
idxRes.show()
</code>
</pre>
- 옵션 : 추후 없던 값이 나타났을 때 무시하거나 오류를 뱉애내는 옵션 지정 가능
<pre>
<code>
valIndexer.SetHandlelnvalid("Skip")
valIndexer.fit(simpleDF).setHandlelnvalid(''skip")
</code>
</pre>

##### 25.6.2 색인된 값을 텍스트로 변환하기 
<pre>
<code>
from pyspark.ml.feature import IndexToString
labelReverse = IndexToString().setInputCol("labelInd")
labelReverse.transform(idxRes).show()
</code>
</pre>

##### 25.6.3 벡터 인덱싱하기
- vectorIndexer는 벡터 내에 존재하는 범주형 변수를 대상으로 하는 유용한 도구
- 입력 벡터 내에 존재하는 범주형 데이터를 자동으로 찾아서 0부터 시작하는 카테고리 색인을 사용하여 범주형 특징으로 변환
- 연속형 변수이지만 값이 적을 경우 범주형으로 인식 할 수 
<pre>
<code>
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.linalg import Vectors
idxIn = spark.createDataFrame([
  (Vectors.dense(1, 2, 3),1),
  (Vectors.dense(2, 5, 6),2),
  (Vectors.dense(1, 8, 9),3)
]).toDF("features", "label")
indxr = VectorIndexer()\
  .setInputCol("features")\
  .setOutputCol("idxed")\
  .setMaxCategories(2)
indxr.fit(idxIn).transform(idxIn).show()
---------------------------------------------> MaxCategories(A) A의 값 이하의 값을 가진 컬럼을 자동으로 범주형으로 인식하여 변환
</code>
</pre>

##### 25.6.4 원-핫 인코딩
<pre>
<code>
from pyspark.ml.feature import OneHotEncoder, StringIndexer
lblIndxr = StringIndexer().setInputCol("color").setOutputCol("colorInd")
colorLab = lblIndxr.fit(simpleDF).transform(simpleDF.select("color"))
ohe = OneHotEncoder().setInputCol("colorInd")
ohe.transform(colorLab).show()
</code>
</pre>

#### 25.7 텍스트 데이터 변환자 

##### 25.7.1 텍스트 토큰화 하기
- 토큰화는 자유형 텍스트를 '토큰' 또는 개별 단어 목록으로 변환하는 프로세스
- Tokenizer 클래스를 사용
<pre>
<code>
from pyspark.ml.feature import Tokenizer
tkn = Tokenizer().setInputCol("Description").setOutputCol("DescOut")
tokenized = tkn.transform(sales.select("Description"))
tokenized.show(20, False)
-> 문장을 단어 단위 (공백으로 인식)로 분리
</code>
</pre>
- RegexTokenizer를 이용하면 정규 표현식을 이용한 Tokenizer  가능

##### 25.7.2 일반적인 단어 제거하기
- 불용어 제거하기
- 대소문자를 구분하지 않지만, 필요하다면 가능
- 스파크 2.2 버전을 기준으로 지원하는 불용어 : danish, dutch , english, Hnnish, ,,, spanish
<pre>
<code>
from pyspark.ml.feature import StopWordsRemover
englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
stops = StopWordsRemover()\
  .setStopWords(englishStopWords)\
  .setInputCol("DescOut")
stops.transform(tokenized).show()
</code>
</pre>

##### 25.7.3 단어 조합 만들기 
- n_gram 을 만들어 분석에 활용
- 문장의 구조와 정보를 기존의 모든 단어를 개별적으로 살펴보는것보다 더 잘 포착하기 위해 사용
- a b c d -> 2 n_gram (bigram) -> (a,b) (b,c) (c,d)

##### 25.7.4 단어를 숫자로 변환하기
- 모델에서 사용하기 위해 단어와 단어 조합수를 산출
- CountVectorizer (출현빈도)
- TF - IDF (가중치 반영, 희귀한 단어에 가중치를 부여, 예를들어 'the' 와 같은 단어는 가중치가 적음) 사용 가능
<pre>
<code>
from pyspark.ml.feature import CountVectorizer
cv = CountVectorizer()\
  .setInputCol("DescOut")\
  .setOutputCol("countVec")\
  .setVocabSize(500)\
  .setMinTF(1)\
  .setMinDF(2)
fittedCV = cv.fit(tokenized)
fittedCV.transform(tokenized).show(10, False)
</code>
</pre>

##### 25.7.5 Word2Vec
- 단어 집합의 벡터 표현을 계산하기 위한 딥러닝 기반 도구
- 비슷한 단어를 벡터 공간에서 서로 가깝게 배치하여 단어를 일반화 (수치화)
- 단어간 관계를 파악하는데 특히 유용
- 참고 자료 : https://ratsgo.github.io/from%20frequency%20to%20semantics/2017/03/30/word2vec/
<pre>
<code>
# Learn a mapping from words to Vectors.
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text",
  outputCol="result")
model = word2Vec.fit(documentDF)
result = model.transform(documentDF)
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))
</code>
</pre>

#### 25.8 특징 조작하기 
##### 25.8.1 주성분 분석 (PCA)
- 데이터의 가장 중요한 측면을 찾는 수학적 기법
- 대규모 입력 데이터 셋에서 총 특징 수를 줄이기 위해 사용
<pre>
<code>
from pyspark.ml.feature import PCA
pca = PCA().setInputCol("features").setK(2)
pca.fit(scaleDF).transform(scaleDF).show(20, False)
</code>
</pre>

#### 25.8.2 상호작용
##### 25.8.3 다항식 전개 
- 모든 입력 컬럼의 상호작용 변수를 생성하는데 사용
<pre>
<code>
from pyspark.ml.feature import PolynomialExpansion
pe = PolynomialExpansion().setInputCol("features").setDegree(2)
pe.transform(scaleDF).show()
</code>
</pre>

#### 25.9 특징선택
##### 25.9.1 ChiSqSelector
- 통계적 검정을 활용하여 예측하려는 레이블과 독립적이지 않은 특징을 식별하고 관련 없는 특징을 삭제
- 카이제곱 겁정을 기반으로 특징을 선택하는 기준은 percentile, numTopfeatures 등 여러가지 방법이 있음

#### 25.10 고급 주제
##### 25.10.1 변환자 저장하기
- 변환자를 개별적으로 유지하려면 장착된 변환자에 wirte 메서드를 사용하고 위치를 지정
<pre>
<code>
fittedPCA = pca.fit(scaleDF)
fittedPCA.write().overwrite().save("/tmp/fittedPCA")
</code>
</pre>
<pre>
<code>
from pyspark.ml.feature import PCAModel
loadedPCA = PCAModel.load("/tmp/fittedPCA")
loadedPCA.transform(scaleDF).show()
</code>
</pre>

##### 25.10.2 사용자 정의 변환자 작성하기
- 사용자 정의 변환자를 작성하는 것은 ML 파이프라인에 적합 시키고, 하이퍼 파라미터 검색에 전달할 수 있는 형식으로 자신의 비지니스 논리 중 일부를 인코딩 하려는 경우 유용

