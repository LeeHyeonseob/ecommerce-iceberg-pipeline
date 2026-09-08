# 도메인 용어

| 용어 | 프로젝트 정의 |
| --- | --- |
| 이벤트 | view, cart, purchase 중 하나인 원본 행동 로그 한 건 |
| purchase event count | purchase 이벤트 수. 주문 수가 아니다. |
| GMV | purchase 이벤트의 `price` 합. 원본에 통화와 quantity가 없다. |
| 퍼널 | `(user_session, product_id)`별 view·cart·purchase 행동을 한 행으로 접은 결과 |
| 세션 내 전환 | 같은 퍼널에서 purchase가 존재하는 상태 |
| cross-session 전환 | 같은 사용자·상품이 다른 세션에서 anchor 이후 30일 안에 구매된 상태 |
| anchor | `coalesce(first_cart_ts, first_view_ts)`로 정한 cross-session 탐색 시작 시각 |
| 최종 이탈률 | 세션 내 구매와 30일 cross-session 구매를 모두 제외한 cart 미전환 비율 |
| lost revenue | 최종 미전환 cart의 최초 cart 가격 합. 실제 손실 매출로 단정하지 않는다. |
| event date | 이벤트 발생 시각에서 파생한 Silver Events 날짜 |
| funnel date | 최초 view, cart, purchase 순으로 선택한 퍼널 시작 날짜 |
| pipeline lag | Kafka 기록 시각부터 Flink S3 적재 시각까지의 차이. event-time 지연이 아니다. |

원본에는 `order_id`, `cart_id`, `quantity`, 통화가 없다. 따라서 주문·장바구니 단위를 복원하거나 통화를 추정하지 않는다.

## 해석과 운영 제약

- 현재 데이터는 2019년 10월 약 4,245만 건이며 관측 기간은 약 31일이다.
- 30일 cross-session window 때문에 종료일에 가까운 이탈률은 상한, 지연 전환은 하한이다.
- 동일 세션·상품의 반복 구매는 Funnel 한 행으로 접히므로 Funnel에서 GMV를 계산할 수 없다.
- category와 brand 대표값 `max`는 시간에 따른 값 변경을 보존하지 않는다.
- `converted_later`는 인과적 attribution이 아니라 사용자·상품·시간 조건 매칭이다.
- `purchase_without_view`는 동일 세션에 view가 없다는 뜻이지 실제로 상품을 보지 않았다는 뜻이 아니다.
- 로컬 Docker 재현 환경이며 AWS S3와 Glue 없이 end-to-end 실행할 수 없다.
- health check는 로그 출력까지 구현되어 있고 임계값 실패와 외부 알림은 미구현이다.
- 독립적인 단위·통합 테스트 suite는 아직 없다.
- README의 행 수와 재작성량은 특정 실행의 관측값이며 고정된 기대값이 아니다.
