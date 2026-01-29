# Slack_table_excel

Slack Webhook 또는 Bot을 사용하여 Airflow를 이용한 자동 알림 보내기
---

본 프로젝트는 알림 목적에 따라 Slack Incoming Webhook과 Slack Bot을 병행하여 사용합니다.

- Webhook 방식
  - Airflow DAG / Task 성공·실패 여부 알림
  - 실행 결과를 Table Markdown 형식으로 가공하여 메시지로 전달
  - 빠르고 가벼운 상태 알림에 적합

- Bot 방식
  - 실행 결과를 엑셀(.xlsx) 파일로 생성하여 Slack에 파일 업로드
  - 데이터 양이 많거나 파일 공유가 필요한 경우에 적합

각 방식의 장점을 살려, 텍스트 기반 알림은 Webhook, 파일 전달은 Bot으로 역할을 분리했습니다.

<예시>

<img width="802" height="327" alt="image" src="https://github.com/user-attachments/assets/d0833f03-ca6b-42de-8c0b-c729a44f7420" />

<img width="715" height="319" alt="image" src="https://github.com/user-attachments/assets/ee6f92c6-9292-455e-b9c1-c107e9b88229" />
