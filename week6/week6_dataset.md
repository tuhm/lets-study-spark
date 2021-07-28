### 11. Dataset 

- 구조적 API. Scala 와 Java 에서만 사용 가능.  
- Data Frame 은 Row 타입의 Dataset이라고 볼 수 있음 (연산에 효율적) Dataset 은 Row 타입 외에도 다양한 타입을 수용할 수 있음
- 사용자가 정의한 타입을 변환하여 분산 방식으로 데이터를 다루기 때문에 연산에는 느림
 
11.1 Dataset 을 사용할 시기 
- DataFrame 기능만으로 수행할 연산을 표현할 수 없는 경우 (복잡한 비즈니스 로직을 단일 함수로 표현해야 할 경우)
- 성능 저하를 감수하더라도 타입안정성을 얻고 싶은 경우 (타입이 유효하지 않은 경우 런타임이 아니라 컴파일 타임에 에러를 발생시킴)
-  단일 노드의 워크로드와 스파크 워크로드에서 전체로우에 대한 다양한 트랜스포메이션을 재사용하려면 (????) Dataset 을 사용해야 함
-  더 적합한 워크로드를 위해서는 Data Frame 과 Dataset 을 혼합하여 사용할 필요가 있을 수 있다 (대량의 DataFrame 기반 ETL 마지막 단계, 또는 트랜스포메이션 맨 처음에)

11.2 Dataset 생성

11.2.1 Java - Encoder 활용 
``` Dataset<class_name> ds = spark.read.parquet(" ~~ ").as(Encoders.bean(class_name.class)); ```

11.2.2 scala - case class 활용
- case class 로 데이터 타입을 지정한 뒤 dataframe 을 해당 class 로 변환 ``` val some_df.as[case_class_name] ```

11.3 Action
- ```collect, take, count``` 같은 액션을 수행할 수 있으며 데이터 타입은 몰라도 됨 

11.4 Transformation
- DataFrame 에서 수행할 수 있는 Transformation + Java 원형 데이터 타입 형태로 저장되기 때문에 그 이상을 수행할 수 있음 

11.4.1 필터링 

11.4.2 매핑

11.5 Join
```joinWith```

11.6 그룹화와 집계 
- groupby, rollup, cube 는 DataFrame 과 동일하며 결과로 Dataframe 을 반환
- ```groupbyKey```


참고:
https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-dataset-vs-sql.html

