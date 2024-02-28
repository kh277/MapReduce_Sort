# 2023년 2학기 데이터베이스시스템 팀프로젝트

### MapReduce 시스템

<br>

## 작동방법

1. 처음 입력 시, 분산처리에 사용할 메모리 크기를 입력한다. 단위는 자동으로 MB로 계산된다.
2. input 텍스트 파일의 이름을 확장자를 포함하여 입력한다. 텍스트 파일은 .cpp와 같은 폴더 내에 있어야 한다.
3. 파일 분할 및 외부 정렬을 자동으로 실행한다.

<br>

## 필수 구현내용
1. Single PC 기반 MapReduce 시스템  
❑ 디스크에 저장된 Key-Value 쌍을 그룹(블록화) 지어서 다루는 기능  
❑ Map, Reduce 인터페이스 제공  
❑ Map과 Reduce 간 Key 별로 Sorting하는 기능

2. 응용 프로그램(Word Count)
   
3. 성능을 높이기 위한 추가기능은 가산점  
❑ 여러 블록을 동시에 처리하는 기능 ✅  
❑ Concurrent Queue 등 ✅  

<br>

## 데이터 처리 흐름
![image](https://github.com/kh277/MapReduce_Sort/assets/113894741/2c315e64-99ce-4533-889d-cd4fda7ea264)

<br>

## Word Count 예시
![image](https://github.com/kh277/MapReduce_Sort/assets/113894741/0d75c792-8652-4a50-9784-6ecd181d2959)  

