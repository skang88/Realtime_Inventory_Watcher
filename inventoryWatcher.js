const sql = require('mssql');
const axios = require('axios');
const moment = require('moment');
require('dotenv').config();

const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: 'sag',
  options: { encrypt: true, trustServerCertificate: true }
};

let pool;

// 데이터베이스 연결 함수
async function connectDB() {
  if (!pool) {
    console.log("🔄 데이터베이스 연결 중...");
    pool = await sql.connect(config);
    console.log("✅ 데이터베이스 연결 성공!");
  }
  return pool;
}

// 재고 확인 및 Slack 메시지 전송 함수
async function checkInventory() {
  console.log("🔍 재고 상태 확인 중...");

  // 감시 시작 메시지 전송
  await sendSlackAlert("🔄 *라인 재고 감시 시작...*");

  try {
    const pool = await connectDB(); // 연결 재사용

    const query = `
      WITH StandbyInventory AS (
          SELECT ITMNO, WARHS AS LINE, SUM(JQTY) AS 스탠바이재고 
          FROM SAG.dbo.MAT_ITMBLPFSUB 
          WHERE WARHS IN ('F01', 'R01', 'C01', 'F31') AND JQTY > 0
          GROUP BY ITMNO, WARHS
      ),
      LotInInventory AS (
          SELECT LINE, ITMNO, SUM(J_QTY) AS 로뜨인재고
          FROM SAG.dbo.PRD_LOTIN 
          WHERE LINE IN ('F01', 'R01', 'C01', 'F31') AND J_QTY > 0
          GROUP BY ITMNO, LINE
      ),
      TotalInventory AS (
          SELECT 
              COALESCE(s.LINE, l.LINE) AS LINE,  
              COALESCE(s.ITMNO, l.ITMNO) AS ITMNO,
              COALESCE(s.스탠바이재고, 0) AS 스탠바이재고,
              COALESCE(l.로뜨인재고, 0) AS 로뜨인재고,
              COALESCE(s.스탠바이재고, 0) + COALESCE(l.로뜨인재고, 0) AS 총재고
          FROM StandbyInventory s
          FULL JOIN LotInInventory l
          ON s.ITMNO = l.ITMNO  
          AND s.LINE = l.LINE
      ),
      ProductionPlanConverted AS (
          SELECT 
              p.RDATE, 
              p.WRK_CD AS LINE,  
              b.CITEM AS ITMNO,  
              SUM(p.PL_QTY * b.[QTY]) AS 일일_소재기준_필요수량,  
              SUM(p.RH_QTY * b.[QTY]) AS 일일_소재기준_사용수량  
          FROM SAG.dbo.PRD_PRDPDPF p  
          JOIN SAG.dbo.BAS_BOM_JORIP b  
              ON p.ITMNO = b.ITMNO
          WHERE p.RDATE = CONVERT(VARCHAR(8), GETDATE(), 112) 
            AND p.WRK_CD IN ('F01', 'R01', 'C01', 'F31')
          GROUP BY p.RDATE, p.WRK_CD, b.CITEM
      )
      SELECT 
          COALESCE(i.LINE, p.LINE) AS LINE,  
          COALESCE(i.ITMNO, p.ITMNO) AS ITMNO,  
          itm.ITM_NM AS 아이템_이름,  
          COALESCE(i.총재고, 0) AS 총재고,  
          COALESCE(i.스탠바이재고, 0) AS 스탠바이재고,  
          COALESCE(i.로뜨인재고, 0) AS 로뜨인재고,  
          COALESCE(p.일일_소재기준_필요수량, 0) AS 일일_필요수량,  
          COALESCE(p.일일_소재기준_사용수량, 0) AS 일일_사용수량,  
          (COALESCE(i.총재고, 0) - COALESCE(p.일일_소재기준_필요수량, 0) + COALESCE(p.일일_소재기준_사용수량, 0)) AS 예상_잔여재고  
      FROM TotalInventory i
      FULL JOIN ProductionPlanConverted p
      ON i.ITMNO = p.ITMNO
      AND i.LINE = p.LINE
      LEFT JOIN SAG.dbo.BAS_ITMSTPF itm  
      ON COALESCE(i.ITMNO, p.ITMNO) = itm.ITMNO
      WHERE (COALESCE(i.총재고, 0) - COALESCE(p.일일_소재기준_필요수량, 0) + COALESCE(p.일일_소재기준_사용수량, 0)) < 0;
    `;

    console.log("🛠 SQL 실행 중...");
    const result = await pool.request().query(query);

    console.log(`📊 조회된 재고 부족 항목: ${result.recordset.length}개`);
    
    if (result.recordset.length > 0) {
      let message = "*🚨 재고 부족 경고! 🚨*\n";
      
      result.recordset.forEach(row => {
        message += `📦 *품번:* ${row.ITMNO} (${row.아이템_이름})\n`;
        message += `🏭 *라인:* ${row.LINE}\n`;
        message += `📊 *총 재고:* ${row.총재고}\n`;
        message += `📉 *필요 수량:* ${row.일일_필요수량}\n`;
        message += `⚠️ *예상 잔여 재고:* ${row.예상_잔여재고}\n\n`;
      });

      await sendSlackAlert(message);
    } else {
      console.log("✅ 모든 재고 충분함");
      await sendSlackAlert("✅ *재고 감시 완료: 모든 재고가 충분합니다.*");
    }

  } catch (err) {
    console.error("❌ Error fetching data:", err);
    await sendSlackAlert("❌ *재고 감시 오류 발생!*");
  }

  // 다음 실행 시간 계산 (현재 시간 + 1시간)
  const nextRunTime = moment().add(1, 'hour').format("HH:mm");

   // 감시 종료 및 다음 실행 예고 메시지 전송
   await sendSlackAlert(`✅ *라인 재고 감시 완료!* 다음 감시는 *${nextRunTime}* 에 시작됩니다. 🕒`);

}

// Slack 웹훅 알림 보내기
async function sendSlackAlert(message) {
  try {
    console.log("🔔 Slack 메시지 전송 중...");
    await axios.post(process.env.SLACK_WEBHOOK, { text: message });
    console.log("✅ Slack 메시지 전송 완료!");
  } catch (error) {
    console.error("❌ Slack 메시지 전송 실패:", error);
  }
}

// 프로세스 종료 감지 (Ctrl + C)
process.on('SIGINT', async () => {
  console.log("\n🛑 감시 프로세스 종료 중...");
  await sendSlackAlert("🔴 *라인 재고 감시 프로그램 종료!*");

  if (pool) {
    console.log("🔌 데이터베이스 연결 해제...");
    await pool.close();
  }

  console.log("👋 프로그램 종료 완료.");
  process.exit(0);
});

// 프로그램 시작 시 Slack 알림
(async () => {
  console.log("🚀 재고 감시 프로그램 시작!");
  await sendSlackAlert("🟢 *라인 재고 감시 프로그램 시작!*");
  checkInventory(); // 첫 번째 감시 실행
})();

// 1시간마다 실행
setInterval(checkInventory, 60 * 60 * 1000);