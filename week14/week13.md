## 26장. 분류
주어진 입력 특징을 사용하여 레이블, 카테고리, 클래스 또는 이산형 변수를 예측

### 26.2 분류 유형
- 이진 분류
- 다중 클래스 분류
- 다중 레이블 분류

- 다중 클래스/레이블 차이는?
  - 다중 클래스 : 주어진 입력에 대해 여러 레이블 중에서 하나의 레이블이 선택되는 방식 (한 사람의 얼굴에 대해 누구인지 예측)
  - 다중 레이블 : 주어진 입력에 대해 여러 레이블을 생성할 수 있는 방식 (여러 사람이 등장하는 사진에 인식된 사람마다 각각 레이블을 할당, 책 징르 분류기 중 액션-판타지 와 같은 복합장르로 예측)

### 26.3 분류 모델
- 제공 분류 모델
  - LR
  - DT
  - RF
  - GBT
* spark는 다중 레이블 예측 지원 x -> 사용하려면 레이블당 하나의 모델 학습시켜 수동으로 조합해야함, 수동 생성 이후 지워 내장 도구 사용 가능

#### 26.3.1 모델 확장성

| 모델 | 최대 특징 수 | 최대 학습 데이터 수 | 최대 타깃 범주 수 |
| ----  | ---- | ---- | ---- |
| LR | 1~1천만개 | 제한x | 특징 수 x 클래수 수 < 1천만개 | 
| DT | 1,000개 | 제한x | 특징 수 x 클래수 수 < 10,000개 |
| RF | 10,000개 | 제한x | 특징 수 x 클래수 수 < 100,000개 |
| GBT | 1,000개 | 제한x | 특징 수 x 클래수 수 < 10,000개 |

* 최대 학습 제한을 없애기 위해 사용되는 방법
- schochastic gradient descent
  - 학습 데이터를 쪼개어 조금만 훑어보고 (mini-batch) gradient descent를 진행
  - loss를 줄여나가는 과정이 best는 아니지만 속력이 빠름
  - ref : https://seamless.tistory.com/38  
- L-BFGS(Limited memory BFGS)

### 26.4 로지스틱 회귀
- 하나의 개별 특징과 특정 가중치를 결합하여 특정 클래스에 속할 확률을 얻는 선형 방법론

#### 하이퍼파라미터
<pre>
<code>
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression()
print lr.explainParams() # see all parameters
lrModel = lr.fit(bInput)

# Results
lowerBoundsOnCoefficients: The lower bounds on coefficients if fitting under bound constrained optimization. The bound matrix must be compatible with the shape (1, number of features) for binomial regression, or (number of classes, number of features) for multinomial regression. (undefined)
lowerBoundsOnIntercepts: The lower bounds on intercepts if fitting under bound constrained optimization. The bounds vector size must beequal with 1 for binomial regression, or the number oflasses for multinomial regression. (undefined)
maxIter: max number of iterations (>= 0). (default: 100)
predictionCol: prediction column name. (default: prediction)
probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities. (default: probability)
rawPredictionCol: raw prediction (a.k.a. confidence) column name. (default: rawPrediction)
regParam: regularization parameter (>= 0). (default: 0.0)
standardization: whether to standardize the training features before fitting the model. (default: True)
threshold: Threshold in binary classification prediction, in range [0, 1]. If threshold and thresholds are both set, they must match.e.g. if threshold is p, then thresholds must be equal to [1-p, p]. (default: 0.5)
thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0, excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold. (undefined)
tol: the convergence tolerance for iterative algorithms (>= 0). (default: 1e-06)
upperBoundsOnCoefficients: The upper bounds on coefficients if fitting under bound constrained optimization. The bound matrix must be compatible with the shape (1, number of features) for binomial regression, or (number of classes, number of features) for multinomial regression. (undefined)
upperBoundsOnIntercepts: The upper bounds on intercepts if fitting under bound constrained optimization. The bound vector size must be equal with 1 for binomial regression, or the number of classes for multinomial regression. (undefined)
weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0. (undefined)

print(lrModel.coefficients)
print(lrModel.intercept)

# Results
[6.848741325749501,0.3535658900824287,14.814900276155212]
-10.22569586428697

</code>
</pre>
- ref : https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.LogisticRegression.html

- family : 이진/다중 클래스 설정 
- elasticNetParam : 알파값 설정, 알파값에 따라 regularization 방식 및 정도 정해짐 
  - alpha = 1, it is an L1 penalty -> Lasso / 변수 축소 
  - alpha = 0  the penalty is an L2 penalty -> Ridge 가중치 축소  
  - 0 < alpha < 1, elastic net / loss function에 regularization term이 coef의 절대값 항(Lasso)과 coef의 제곱항(Ridge)이 함께 사용  
    - 상관성이 높은 변수 사이에서 Lasso를 사용하면 그 중 하나를 랜덤으로 선택하여 계수 축소하기 때문에, 실제로 가장 중요한 변수가 선택되지 않을 리스크 있음  
    - Elastic net의 경우, 상관성 높은 다수의 변수들을 모두 선택하거나 제거 -> group effect 유도  
    - 결론적으로 변수 선택 기능도 있음
![스크린샷 2021-11-08 07 52 29](https://user-images.githubusercontent.com/36292871/140664888-6b1ca62f-e81a-4939-8a08-b8791fa9b303.png)
![스크린샷 2021-11-08 08 03 23](https://user-images.githubusercontent.com/36292871/140665183-2c4eafc3-b6ce-433b-89c4-d8e723345a5d.png)

- fitIntercept : intercept 적합 여부, 스케일링 안 했다면 true 설정 필요
- regParam : regularization lambda 값 / 0~1 사이 설정
- standardization : scaling 적용 여부

#### 학습 파라미터
- maxIter : 총 학습 반복 횟수
- tol : 학습 반복 임계값 설정 / converge해서 loss가 이 임계치만큼 줄지 않으면 학습 중지
- weightCol : 사전 가중치 부여

#### 예측 파라미터
- threshold : 클래스 예측 확률 임곗값 (default=0.5)
- thresholds : 다중 클래스 경우

### 26.5 의사결정나무
- 주어진 샘플들 사이에 존재하는 패턴을 예측 가능한 규칙들의 조합으로 나타내는 알고리즘
- 과적합 방지 필요

#### 하이퍼파라미터
- maxDepth(= max_depth) : 의사결정 나무 최대 깊이 지정 / 과적합 방지
- maxBins : 연속형 변수를 범주형 변수로 변환할시 binning의 최대 갯수 / 클수록 세분화된 분석 가능 
- impurity(= criterion) : 나무 분기의 기준이 되는 불순도 측정 기준 
  - entropy
  - gini
  - ref : https://ratsgo.github.io/machine%20learning/2017/03/26/tree/
- minInfoGain(= min_impurity_decrease) : 나무 분기를 위한 최소 정보 획득 정도 설정 / default : 0 / 과적합 방지
- minInstancePerNode(= min_sample_leaf) : 나무 분기를 위해 갈라진 노드에 필요한 최소 instance 갯수 / 과적합 방지 

#### 학습 파라미터
- checkpointInterval : 학습 과정 동안 진행되는 모델의 작업 내용 저장, 클러스터 내 특정 노드가 충돌할 경우에도 이전까지 진행된 작업 내용 복구 가능
  - ex. 10으로 지정할 경우, 모델이 10번 반복될 때마다 checkpoint 생김
  - 미사용시, -1로 설정
  - checkpointDir(체크포인트 디렉토리 설정) / useNodeIdCache=true 설정 필요

<pre>
<code>
from pyspark.ml.classification import DecisionTreeClassifier
dt = DecisionTreeClassifier()
print dt.explainParams()
dtModel = dt.fit(bInput)

# Results
cacheNodeIds: If false, the algorithm will pass trees to executors to match instances with nodes. If true, the algorithm will cache node IDs for each instance. Caching can speed up training of deeper trees. Users can set how often should the cache be checkpointed or disable it by setting checkpointInterval. (default: False)
checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext. (default: 10)
featuresCol: features column name. (default: features)
impurity: Criterion used for information gain calculation (case-insensitive). Supported options: entropy, gini (default: gini)
labelCol: label column name. (default: label)
maxBins: Max number of bins for discretizing continuous features.  Must be >=2 and >= number of categories for any categorical feature. (default: 32)
maxDepth: Maximum depth of the tree. (>= 0) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 5)
maxMemoryInMB: Maximum memory in MB allocated to histogram aggregation. If too small, then 1 node will be split per iteration, and its aggregates may exceed this size. (default: 256)
minInfoGain: Minimum information gain for a split to be considered at a tree node. (default: 0.0)
minInstancesPerNode: Minimum number of instances each child must have after split. If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Should be >= 1. (default: 1)
predictionCol: prediction column name. (default: prediction)
probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities. (default: probability)
rawPredictionCol: raw prediction (a.k.a. confidence) column name. (default: rawPrediction)
seed: random seed. (default: 956191873026065186)
</code>
</pre>


### 26.6 랜덤포레스트 & 그래디언트 부스티드 트리
- Tree 계열 알고리즘 
- 두 알고리즘 모두 앙상블 기법이지만 RF는 각 트리의 출력을 평균하여 사용하고, GBT는 각각의 트리에 가중치가 부여됨
- Spark에서 GBT는 Binary Classification만 지원

#### RF 하이퍼파라미터
- numTrees(=n_estimators) : 학습 트리 개수
- featureSubsetStrategy(=max_features)
  - RF의 가장 큰 특징은, feature를 랜덤샘플링하여 트리들 간의 correlation을 줄이는 것인데, 이 개별 트리 subset의 후보 feature를 몇 개까지 사용할 것인가를 선택
  - auto / all / sqrt / log2 / n 값 지정
    - 0과 1 사이의 n (ex. n=0.5) subset을 만들 때 총 feature의 0.5배수만 사용하여 개별 트리를 생성
- categoricalFeatureInfo 
- minInfoGain(=min_impurity_decrease)
- minInstancesPerNode(=min_sample_leat)
- subsamplingRate 
- maxBins
- impurity
- seed

#### GBT only 하이퍼파라미터
- lossType(=loss) : 최적화 loss function / 현재 logistic loss만 지원
- maxIter(n_estimators)
- stepsize(=learning_rate)

#### RF & GBT 학습 파라미터
- checkpointInterval (학습 파라미터)

#### RF & GBT input & output columns
- input params
  - labelCol : 예측할 label feature 지정
  - featuresCol : feature vector
- output 
  - predictionCol : 예측된 label
  - rawPredictionCol : 각 라벨에 대한 예측 확률 값 -> 현재 GBT에선 지원 안됨 
  - probabilityCol : 각 라벨에 대한 예측 확률 값을 normalized한 값 -> 현재 GBT에선 지원 안됨 


<pre>
<code>
from pyspark.ml.classification import RandomForestClassifier
rfClassifier = RandomForestClassifier()
print(rfClassifier.explainParams())
trainedModel = rfClassifier.fit(bInput)

## Results
featuresCol: features column name. (default: features)
impurity: Criterion used for information gain calculation (case-insensitive). Supported options: entropy, gini (default: gini)
labelCol: label column name. (default: label)
maxBins: Max number of bins for discretizing continuous features.  Must be >=2 and >= number of categories for any categorical feature. (default: 32)
maxDepth: Maximum depth of the tree. (>= 0) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 5)
maxMemoryInMB: Maximum memory in MB allocated to histogram aggregation. If too small, then 1 node will be split per iteration, and its aggregates may exceed this size. (default: 256)
minInfoGain: Minimum information gain for a split to be considered at a tree node. (default: 0.0)
minInstancesPerNode: Minimum number of instances each child must have after split. If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Should be >= 1. (default: 1)
numTrees: Number of trees to train (>= 1). (default: 20)
predictionCol: prediction column name. (default: prediction)
probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities. (default: probability)
rawPredictionCol: raw prediction (a.k.a. confidence) column name. (default: rawPrediction)
seed: random seed. (default: -5387697053847413545)
subsamplingRate: Fraction of the training data used for learning each decision tree, in range (0, 1]. (default: 1.0)
</code>
</pre>

<pre>
<code>
from pyspark.ml.classification import GBTClassifier
gbtClassifier = GBTClassifier()
print(gbtClassifier.explainParams())
trainedModel = gbtClassifier.fit(bInput)

## Results
cacheNodeIds: If false, the algorithm will pass trees to executors to match instances with nodes. If true, the algorithm will cache node IDs for each instance. Caching can speed up training of deeper trees. Users can set how often should the cache be checkpointed or disable it by setting checkpointInterval. (default: False)
checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext. (default: 10)
featureSubsetStrategy: The number of features to consider for splits at each tree node. Supported options: 'auto' (choose automatically for task: If numTrees == 1, set to 'all'. If numTrees > 1 (forest), set to 'sqrt' for classification and to 'onethird' for regression), 'all' (use all features), 'onethird' (use 1/3 of the features), 'sqrt' (use sqrt(number of features)), 'log2' (use log2(number of features)), 'n' (when n is in the range (0, 1.0], use n * number of features. When n is in the range (1, number of features), use n features). default = 'auto' (default: all)
featuresCol: features column name. (default: features)
labelCol: label column name. (default: label)
lossType: Loss function which GBT tries to minimize (case-insensitive). Supported options: logistic (default: logistic)
maxBins: Max number of bins for discretizing continuous features.  Must be >=2 and >= number of categories for any categorical feature. (default: 32)
maxDepth: Maximum depth of the tree. (>= 0) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 5)
maxIter: max number of iterations (>= 0). (default: 20)
maxMemoryInMB: Maximum memory in MB allocated to histogram aggregation. If too small, then 1 node will be split per iteration, and its aggregates may exceed this size. (default: 256)
minInfoGain: Minimum information gain for a split to be considered at a tree node. (default: 0.0)
minInstancesPerNode: Minimum number of instances each child must have after split. If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Should be >= 1. (default: 1)
predictionCol: prediction column name. (default: prediction)
seed: random seed. (default: 3504127614838123891)
stepSize: Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator. (default: 0.1)
subsamplingRate: Fraction of the training data used for learning each decision tree, in range (0, 1]. (default: 1.0)
</code>
</pre>

### Spark ML 예시 코드
#### 데이터 로드 및 DataFrame 생성
<pre>
<code>
val creditDf = spark.read.format("csv")
  .option("header", value = true)
  .option("delimiter", ",")
  .option("mode", "DROPMALFORMED")
  .schema(schema)
  .load(getClass.getResource("/credit.csv").getPath)
  .cache()
creditDf.printSchema()
</code>
</pre>

#### Add feature columns
<pre>
<code>
## Array column 지정
val cols = Array("balance", "duration", "history", "purpose", "amount", "savings", "employment", "instPercent", "sexMarried",
  "guarantors", "residenceDuration", "assets", "age", "concCredit", "apartment", "credits", "occupation", "dependents", "hasPhone",
  "foreign")

// VectorAssembler to add feature column
// input columns - cols
// feature column - features

## Assembler 통해서 위에서 지정한 변수들을 vector로 묶어줌
val assembler = new VectorAssembler()
  .setInputCols(cols)
  .setOutputCol("features")
val featureDf = assembler.transform(creditDf)
featureDf.printSchema()
</code>
</pre>

#### Add label column
<pre>
<code>
## Stringindexer를 통해 label 지정
val indexer = new StringIndexer()
  .setInputCol("creditability")
  .setOutputCol("label")
val labelDf = indexer.fit(featureDf).transform(featureDf)
labelDf.printSchema()
</code>
</pre>

#### Build RF Model
<pre>
<code>
val seed = 5043
val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

// train Random Forest model with training data set
val randomForestClassifier = new RandomForestClassifier()
  .setImpurity("gini")
  .setMaxDepth(3)
  .setNumTrees(20)
  .setFeatureSubsetStrategy("auto")
  .setSeed(seed)
val randomForestModel = randomForestClassifier.fit(trainingData)


val predictionDf = randomForestModel.transform(testData)
predictionDf.show(10)
</code>
</pre>

#### Build Model with Pipeline
<pre>
<code>
// we run marksDf on the pipeline, so split marksDf

## creditDF -> indexing 하기 이전 spark dataframe
val Array(pipelineTrainingData, pipelineTestingData) = creditDf.randomSplit(Array(0.7, 0.3), seed)

// VectorAssembler and StringIndexer are transformers
// LogisticRegression is the estimator
val stages = Array(assembler, indexer, randomForestClassifier)

// build pipeline
val pipeline = new Pipeline().setStages(stages)
val pipelineModel = pipeline.fit(pipelineTrainingData)

// test model with test data
val pipelinePredictionDf = pipelineModel.transform(pipelineTestingData)
pipelinePredictionDf.show(10)
</code>
</pre>

#### Model Tuning with CrossValidator
<pre>
<code>
val paramGrid = new ParamGridBuilder()
  .addGrid(randomForestClassifier.maxBins, Array(25, 28, 31))
  .addGrid(randomForestClassifier.maxDepth, Array(4, 6, 8))
  .addGrid(randomForestClassifier.impurity, Array("entropy", "gini"))
  .build()

// define cross validation stage to search through the parameters
// K-Fold cross validation with BinaryClassificationEvaluator
val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(5)

// fit will run cross validation and choose the best set of parameters
// this will take some time to run
val cvModel = cv.fit(pipelineTrainingData)

// test cross validated model with test data
val cvPredictionDf = cvModel.transform(pipelineTestingData)
cvPredictionDf.show(10)

val cvAccuracy = evaluator.evaluate(cvPredictionDf)
println(cvAccuracy)
</code>
</pre>

#### Evaluate Model
<pre>
<code>
val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setMetricName("areaUnderROC")
  
val accuracy = evaluator.evaluate(predictionDf)
println(accuracy)
</code>
</pre>


#### RF 예시 코드
<pre>
<code>
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Load and parse the data file, converting it to a DataFrame.
data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.

## label 인덱싱을 통해 label columns을 label index로 만들어줌 
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.

## feature 인덱싱을 통해 feature column을 feature 
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)

# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages[2]
print(rfModel)  # summary only
</code>
</pre>

#### GBT 예시 코드
<pre>
<code>
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Load and parse the data file, converting it to a DataFrame.
data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a GBT model.
gbt = GBTClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", maxIter=10)

# Chain indexers and GBT in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, gbt])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

gbtModel = model.stages[2]
print(gbtModel)  # summary only
</code>
</pre>

## 실제 사용 코드 참고
https://medium.com/rahasak/random-forest-classifier-with-apache-spark-c63b4a23a7cc


### 26.7 나이브 베이즈
#### 하이퍼파라미터
- modelType : 이진/다항 분류 선택
- weightCol : 변수에 대한 가중치 부여

<pre>
<code>
featuresCol: features column name. (default: features)
labelCol: label column name. (default: label)
modelType: The model type which is a string (case-sensitive). Supported options: multinomial (default) and bernoulli. (default: multinomial)
predictionCol: prediction column name. (default: prediction)
probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities. (default: probability)
rawPredictionCol: raw prediction (a.k.a. confidence) column name. (default: rawPrediction)
smoothing: The smoothing parameter, should be >= 0, default is 1.0 (default: 1.0)
thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0, excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold. (undefined)
weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0. (undefined)
</code>
</pre>

### 26.8 분류와 자동 모델 튜닝을 위한 평가기
- 위 예시 코드 참고

### 26.10 일대다 분류기
- 다중 클래스 분류 지원하지 않을 경우엔, 1-vs-rest 분류기 활용하여 binary classification으로 바꿔줌
- binary classification 중 각각의 target class에 대한 확률값이 제일 높은 class를 선택
