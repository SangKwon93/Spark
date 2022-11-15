# Key-Value RDD
- Key를 기준으로 고차원적인 연산(집계-aggregation)
- 다양한 집계 연산 가능
ex) 영화 장르별 평균 점수 계산

## Reduction 연산
- Reduction은 key값을 기준으로 데이터 묶어서 처리 -> groupBy
- reduceByKey(<task>) -key를 기준으로 task 처리
- groupByKey() - key를 기준으로 value 묶어 주기
- sortByKey()- key를 기준으로 정렬
- keys() - key 추출
- values() - value 추출

## 데이터 다루는 팁
- Key에 대한 변경이 일어나지 않는다면? **mapValues() 함수 사용!(대부분)** => 파티션 유지 할 수 있다.
   => 셔플링을 방지하기 위해
- mapValues(), flatMapValues() -> value만 다루는 연산들이지만 RDD에서 key는 유지된다.

## Spark Operation 
- Transformations = 새로운 RDD 반환 - 지연 실행
- Actions = 연산 결과 출력 및 저장 - 즉시 실행

## Transformations
- Narrow Transformation
    - 1:1 변환 => 하나의 열을 조작하기 위해 다른 열 및 파티션의 데이터를 사용할 필요가 없다.
    - 로컬에서만 돌아가면 되기 때문에 빠르다.
    - 셔플링이 자주 일어나지 않는다.
    ex) filter(), map(), flatMap(), sample(), union()
- Wide Transformation
    - 셔플링 - 결과RDD의 파티션에 다른 파티션의 데이터가 들어갈 수 있다.
    - Intersection , join, distinct, cartesian, reduceByKey, groupByKey(), sort


    # `많은 리소스를 요구한다.`
    ==> 우리는 최소화,최적화 하는 것을 목적으로 해야한다.

