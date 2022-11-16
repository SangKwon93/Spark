## 스파크 클러스터

### 클러스터의 구조
- 항상 데이터가 여러 곳에 분산
- 같은 연산 이어도 여러 노드에 걸쳐서 실행될 수 있다.
- spark master(namenode)- worker(datanode)
- **데이터의 양을 줄이는 것이 중요** => spark를 고성능으로 활용하기 위해서는 ``데이터 계산의 양을 최소화``하는 것 
<br>
- client(jupyternotebook)
    - 작업을 만들어주는 역할 
    - jupyter와 같은 client에서 실행하게 되면 task가 드라이버프로그램화되어 sparkcontext(master node)로 넘어간다. 
    - (비유적)``고객이 인력소장(master)에게 작업내용(드라이버프로그램)넘기고 인력 소장이 노동자들에게 일을 분배``
<br>
- Master node 
    - SparkContext(가 있는 환경을 의미) 생성하고 이를 통해 RDD 생성
    - 모든 프로세스를 조직하며 메인 프로세스를 수행
    - ``Transformations, actions를 생성하고 worker에 전송``
<br>
- Worker node
    - ``실제 연산이 일어나는 노드``
    - ``Master가 조직한 task를 수행``
      (cluster manager를 통해서 정한 작업내역들을 worker node가 수행)
<br>
- Executor
    - ``executor는 master node가 분배시킨 작업들을 수집하는 역할``
    - task를 실행하고 데이터를 저장하고 그 결과를 드라이브 프로그램에 전송
    - 연산하며 필요한 저장공간을 제공하기 위해 cache를 가지고 있다.
<br>
- Cluster Manager
    - ``수행되는 작업들에 대한 스케줄링 담당``
    - 클러스터 전반에 대한 자원 관리
    <br>
### 스파크 프로그램 실행 과정
1. 드라이버 프로그램이 SparkContext를 이용해 Spark Application을 생성
2. Spark Context가 Cluster Manager에 연락
    - 어떤 작업을 어떤 worker 들에게 작업을 전송할 지 cluster manager에게 알려줘야 
3. cluster manager는 worker에게 자원을 할당(작업을 분배하는)
4. Worker Node들의 Executor들을 수집
    - ``executor가 작업을 수집하고 각각의 executor는 cluster manager가 수집``
    - ``cluster manager -> executor수집 -> task 수집``
5. Executor는 연산을 수행하고 데이터를 저장
    - Executor가 cluster manager에 의해 수집이 되고 자원 할당이되면 연산을 수행하고 데이터를 디스크에 저장
<br>
6. **간단하게**
    - 마스터가 계획을 다 작성해서 작업내역을 클러스터매니저에게 보내면 클러스터 매니저는 worker에 excutor들을 수집해서 작업 내역들을 알려준다.
    - excutor는 task를 수집하여 각각의 쓰레드에 작업을 맡긴다.
    - sparkcontext가 executor들에게 수행할 task들을 실제 프로그램을 worker node에게 전송 
    - 작업이 끝나고 결과물을 디스크에 저장
    - worker는 master에게 보고하는 개념
      (수행된 결과물을 다시 드라이버 프로그램에 보내게된다) 









