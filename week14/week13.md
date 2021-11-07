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
- L-BFGSS(Limited memory BFGS)

### 26.4 로지스틱 회귀
- 하나의 개별 특징과 특정 가중치를 결합하여 특정 클래스에 속할 확률을 얻는 선형 방법론
- 
#### 26.4.1 하이퍼파라미터
<pre>
<code>
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression()
print lr.explainParams() # see all parameters
lrModel = lr.fit(bInput)

print(lrModel.coefficients)
print(lrModel.intercept)
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
    - - 결론적으로 변수 선택 기능도 있음
![스크린샷 2021-11-08 07 52 29](https://user-images.githubusercontent.com/36292871/140664888-6b1ca62f-e81a-4939-8a08-b8791fa9b303.png)
![스크린샷 2021-11-08 08 03 23](https://user-images.githubusercontent.com/36292871/140665183-2c4eafc3-b6ce-433b-89c4-d8e723345a5d.png)

- fitIntercept : intercept 적합 여부, 스케일링 안 했다면 true 설정 필요
- regParam : regularization lambda 값 / 0~1 사이 설정
- standardization : scaling 적용 여부

#### 26.4.2 학습 파라미터
- maxIter : 총 학습 반복 횟수
- tol : 학습 반복 임계값 설정
- weightCol : 사전 가중치 부여
