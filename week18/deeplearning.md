# 딥러닝

이 장에서는 스파크에 내장된 패키지 보다는 스파크에 활용할 수 있는 딥러닝 관련 외부 라이브러리의 엄청난 혁신에 중점을 두고 살펴볼 예정

31.1 딥러닝이란
- 신경망이란 가중치와 활성화 함수를 가진 노드로 구성된 그래프 
- 신경망의 최종 목표는 네트워크 상의 각 노드 사이의 연결과 그에 대한 가중치 및 각 노드의 값을 적절히 조정하여 어떤 값이 입력 되었을 때 특정한 출력값과 연관 시킬 수 있도록 네트워크를 학습시키는 것

31.2 스파크에서 딥러닝을 사용하는 방법
- 추론/ 특징 생성 & 전이학습 / 모델학습
[1] 추론 
- 이미 학습된 모델을 스파크로 가져와서 대용량 데이터 셋에 병렬로 적용 
- Pyspark를 사용하면 맵 함수를 사용해서 텐서플로나, 파이토치와 같은 프레임워크를 호출하여 분산 처리를 통한 추론 가능

[2] 특징 생성과 전이 학습
- 특징을 생성하는 데 사용
- 학습한 분류기를 새로운 문제를 하결 할 수 있는 새로운 모델을 학습하는데 활용하는 것을 "전이 학습" (transfer learning)
- 전이 학습은 본래 분석하고자 하는 학습 데이터가 충분하지 않은 경우 사용하면 특히 유용

[3] 모델 학습
- 스파크 클러스터를 사용하여 단일 모델에 대한 학습을 여러 서버에서 병렬 처리하고, 각 서버 간 통신을 통해 최종 결과를 업데이트
- 특정 라이브러리를 사용하여 다양한 모델 객체를 병렬로 학습 시키고 다양한 모델 아키텍처와 하이퍼파라미터를 검토하여 최종 모델 선택과 최적화 과정을 효율적으로 수행 

3.1 딥러닝 라이브러리

31.3.1 MLlib에서 지원하는 신경망
- ml.calssification.MultilayerPerceptronClassifier 클래스의 다층 퍼셉트론 분류기 : 단일 심층 학습 알고리즘 지원. 상대적으로 얕은 네트워크를 학습하도록 제한
- 이 클래스는 기존 딥러닝 기반 특징 추출기를 사용하여 전이 학습을 할 때 분류 모델의 마지막 몇 개 계층을 학습하는데 가장 유용

31.3.2 텐서 프레임
- Spark DataFrame과 텐서플로 간에 데이터 송수신을 쉽게 하도록 도와주는 추론 및 전이 학습 지향 라이브러리
- 빠른 데이터 전송 및 초기 구동 비용에 대한 상쇄 효과로 파이썬 맵 함수를 호출 하는 것보다 효율적

31.3.3. BigDL
- 딥러닝 모델 빠른 적용과 모델의 분산 학습 지원
- 주로 CPU 활용에 최적화되어 있음

31.3.4 TensorFlowOn Spark
- 텐서플로 모델을 병렬로 학습시키는데 사용하는 라이브러리
- Spark job 내에서 텐서플로의 분산 모드를 실행 시키고, 스파크 RDD 또는 DataFrame 데이터를 텐서플로 잡에 자동으로 공급

31.3.5 DeepLearning4J
- 단일 노드 및 분산 학습 옵션을 모두 제공하는 java 및 Scala의 오픈소스 이자 분산 딥러닝 프로젝트
- JVM용으로 설계되어 파이썬을 개발 프로세스에 추가 하지 않으려는 사용자 그룹에 편의성 제공

31.3.6 딥러닝 파이프라인
- 딥러닝 기능을 스파크의 ML 파이프라인 API에 통합시킨 데이터브릭스의 오픈소스 패키지
- 딥러닝 프레임워크를 표준 스파크 API 통합하여 사용하기 쉽게 만듬
- 모든 연산을 분산 처리


--> ML 파이프라인 API에서 모든 딥러닝 기능을 사용할 수 있는 것이 목표! 2018

31.4 예제
- 딥러닝 파이프라인에는
1. Spark Dataframe 에서 이미지 처리 가능
2. 대규모 딥러닝 모델을 이미지나 텐서 데이터에 적용 가능
3. 사전 학습된 딥러닝 모델을 사용하여 전이 학습 가능
4. 모델을 스파크 SQL 함수로 내보내 모든 사용자가 딥러닝을 쉽게 이용 가능
5. ML 파이프라인을 통해 분산 딥러닝 하이퍼 파라미터 튜닝 가능

31.4.1 설정하기

31.4.2 이미지와 Data Frame

from pyspark.ml.image import ImageSchema
img_dir = '/data/deep-learning-images/'
sample_img_dir = img_dir + "/sample"
#sample_img_dir = '/user/fp10186/21652746_cc379e0eea_m.jpg'
image_df = ImageSchema.readImages(sample_img_dir)
image_df.printSchema()
root
 |-- image: struct (nullable = true)
 |    |-- origin: string (nullable = true)
 |    |-- height: integer (nullable = false)
 |    |-- width: integer (nullable = false)
 |    |-- nChannels: integer (nullable = false)
 |    |-- mode: integer (nullable = false)
 |    |-- data: binary (nullable = false)

31.4.3 전이 학습

- DeepImageFeatureizer 라는 변환자를 활용. 이미지 패턴을 식별하는데 사용되는 강력한 신경망인 인셉션이라는 사전학습 된 모델을 활용. 
- 이 라이브러리를 통해 우리 데이터에 맞게 수정 (전이학습)

- 코드 실습
from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import lit
from sparkdl.image import imageIO
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from sparkdl import DeepImageFeaturizer

tulips_df = ImageSchema.readImages(img_dir + "/tulips").withColumn("label", lit(1))
daisy_df = imageIO.readImagesWithCustomFn(img_dir + "/daisy", decode_f=imageIO.PIL_decode).withColumn("label", lit(0))
tulips_train, tulips_test = tulips_df.randomSplit([0.6, 0.4])
daisy_train, daisy_test = daisy_df.randomSplit([0.6, 0.4])
train_df = tulips_train.unionAll(daisy_train)
test_df = tulips_test.unionAll(daisy_test)
# 메모리 오버헤드를 줄이기 위해 파티션을 나눕니다.
train_df = train_df.repartition(100)
test_df = test_df.repartition(100)

featurizer = DeepImageFeaturizer (inputCol="image", outputCol="features", modelName="InceptionV3") ### ML 변환자 
lr = LogisticRegression(maxIter=20, regParam=0.05, elasticNetParam=0.3, labelCol="label")
p = Pipeline(stages=[featurizer, lr])

p_model = p.fit(train_df)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

tested_df = p_model.transform(test_df)
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(tested_df.select("prediction", "label"))))

31.4.4 인기 있는 표준 모델 사용하기
- 딥러닝 파이프라인은 케라스에 포함된 다양한 표준모델을 지원
- 예시
from pyspark.ml.image import ImageSchema
from sparkdl import DeepImagePredictor
image_df = ImageSchema.readImages(sample_img_dir)
predictor = DeepImagePredictor(inputCol="image", outputCol="predicted_labels", modelName="InceptionV3", decodePredictions=True, topK=10)
predictions_df = predictor.transform(image_df)
- 딥러닝 파이프 라인은 스파크를 사용하여 분산 처리 방식으로 케라스 모델을 적용하도록 지원
- 딥러닝 파이프 라인은 텐서플로와의 통합을 통해 텐서플로 기반 이미지 조작하는 사용자 정의 변환자를 만드는데도 사용 가능
- 모델을 SQL 함수로 전개하여 SQL 이용자가 딥러닝 모델을 사용 할 수 있도록 함
- 예시
from keras.applications import InceptionV3
from sparkdl.udf.keras_image_model import registerKerasImageUDF
from keras.applications import InceptionV3

registerKerasImageUDF("my_keras_inception_udf", InceptionV3(weights="imagenet"))
