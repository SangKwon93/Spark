# Shuffling 과 Partition 내용 정리
## Key-Value RDD Operations & Joins

- Transformations
    - groupByKey
    - reducebyKey
    - mapValues
    - keys
    - join leftOuterJoin
- Actions
    - countByKey

### Shuffling
- 데이터를 그룹화 할 떄 데이터를 한 노드에서 다른 노드로 옮길때 발생한다.(파티션을 넘나드는 것을 셔플링)
- 데이터가 key에 의해서 어딘가에 저장(적재)된다.
- 같은 key는 같은 파티션에 있는 것이 좋다. -> 병렬처리 수월
- `네트워크 연산 시(다른 노드로 파티션을 넘나들 때) 성능을 많이 저하시킨다.`

### shuffle을 일으킬 수 있는 작업들
- 대부분의 연산이 `데이터를 합치거나 하나로 줄이는 작업들`에서 많이 일어난다.
ex) join, GroupByKey, ReduceByKey, CombineByKey,   
CombineByKey, Distinct, Intersection, Repartition, Coalesce 

### Shuffle 최소화 하기
- 내부적으로 spark가 신경쓴다.
- (최선)`둘 다 파티션과 캐싱을 조합해서 최대한 로컬 환경에서 연산이 실행되도록 하는 방식!`
- (차선) 미리 파티션을 만들어 두고 캐싱 후 reduceByKey를 실행
- `함부로 그룹핑하면 좋지 않다!!!`==> 속도 저하

### Partition의 목적
- 쿼리가 같이 되는 데이터를 최대한 옆에 두어 검색 성능 향상 시키기
- `Key-Value RDD에만 의미가 있다.`
- 파티션 안쪽은 비슷한 key끼리 모이게 되어있다.

### Partition의 특징
- RDD는 쪼개져서 여러 파티션에 저장된다.
    - `RDD는여러개의 분산처리 되어있는 데이터를 하나처럼 보이게 해주는 역할`
- 하나의 노드는 여러 개의 파티션을 가질 수 있다.

### `for문은 직렬처리기 때문에 효과가 좋지 않다!!!`
### 그룹핑하는건 좋은데 후처리를 for 문으로 처리를 하면 효과가 좋지않다.

- `for문 쓰는순간 병렬처리 x`  
    - 파티션 내에서 변환수행하는 것이 아니라 드라이버 프로그램에서 하겠다.  
    - `즉, worker node 에서 일어나는 작업이 아니라  master node에서 작업을 하는거 -> 속도 느려진다.`  

