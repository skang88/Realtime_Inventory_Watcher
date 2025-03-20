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
  console.log("🔍 로뜨인 재고 상태 확인 중...");

  await sendSlackAlert("🔄 *로뜨인 재고 감시 시작...*");

  try {
    const pool = await connectDB();

    const query = `
      WITH REQUIRED_MATERIAL AS (
          SELECT 
              P.RDATE,
              P.WRK_CD AS LINE,
              P.SERNO,
              P.ITMNO,
              P.PL_QTY,
              P.RH_QTY,
              (P.PL_QTY - P.RH_QTY) AS REMAIN_QTY,  
			  COALESCE(NULLIF(RTRIM(P.WRKSTS), ''), 'PENDING') AS WRKSTS,
              B.CITEM,
              B.QTY AS BOM_QTY,
              ((P.PL_QTY - P.RH_QTY) * B.QTY) AS REQUIRED_MATERIAL_QTY 
          FROM SAG.dbo.PRD_PRDPDPF P
          JOIN BAS_BOM_JORIP B
              ON P.WRK_CD = B.LINE 
              AND P.ITMNO = B.ITMNO
          WHERE P.RDATE = CONVERT(VARCHAR(8), GETDATE(), 112) 
      ),
      CURRENT_MATERIAL AS (
          SELECT 
              LINE, 
              ITMNO AS CITEM,  
              SUM(J_QTY) AS CURRENT_MATERIAL_QTY 
          FROM SAG.dbo.PRD_LOTIN 
          WHERE LINE IN ('F01', 'R01', 'C01', 'F31') AND J_QTY > 0
          GROUP BY ITMNO, LINE
      )
      SELECT 
          RM.RDATE,
          RM.LINE AS 라인,
          RM.SERNO AS 작업순번,
          RM.WRKSTS,
          RTRIM(RM.ITMNO) AS ITMNO,
          RM.REMAIN_QTY,
          RM.CITEM AS 품번,
          ITM.ITM_NM AS 품명,
          RM.REQUIRED_MATERIAL_QTY AS 필요수량,
          COALESCE(CM.CURRENT_MATERIAL_QTY, 0) AS 현재수량,
          (COALESCE(CM.CURRENT_MATERIAL_QTY, 0) - RM.REQUIRED_MATERIAL_QTY) AS 부족수량 
      FROM REQUIRED_MATERIAL RM
      LEFT JOIN CURRENT_MATERIAL CM
          ON RM.LINE = CM.LINE 
          AND RM.CITEM = CM.CITEM
      LEFT JOIN SAG.dbo.BAS_ITMSTPF ITM  
          ON RM.CITEM = ITM.ITMNO  
      WHERE (RM.REQUIRED_MATERIAL_QTY - COALESCE(CM.CURRENT_MATERIAL_QTY, 0)) > 0
      ORDER BY 
          CASE 
              WHEN RM.WRKSTS = 'W' THEN 1  -- 현재 작업 중인 것(W 상태) 먼저
              ELSE 2                        -- 다음 작업 예정 (값 없음)
          END,
          RM.LINE, RM.SERNO;
    `;

    console.log("🛠 SQL 실행 중...");
    const result = await pool.request().query(query);

    console.log(`📊 조회된 로뜨인 재고 부족 항목: ${result.recordset.length}개`);

    if (result.recordset.length > 0) {
      let message = "*🚨 로뜨인 재고 부족 경고! 🚨*\n";
      
      // ✅ 현재 작업 중 (W 상태)
      const workingItems = result.recordset.filter(row => row.WRKSTS === 'W');
      if (workingItems.length > 0) {
        message += "🔹 *현재 작업 중*\n";
        workingItems.forEach(row => {
          message += `📦 *품번:* ${row.품번} (${row.품명})\n`;
          message += `🏭 *라인:* ${row.라인} (${row.ITMNO})\n`;
          message += `🏭 *작업순번:* ${row.작업순번}\n`;
          message += `📊 *현재 수량:* ${row.현재수량}\n`;
          message += `📉 *필요 수량:* ${row.필요수량}\n`;
          message += `⚠️ *부족 수량:* ${row.부족수량}\n\n`;
        });
      }

      // ✅ 다음 작업 예정 (작업 상태 값 없음)
      const pendingItems = result.recordset.filter(row => row.WRKSTS === "PENDING");
      if (pendingItems.length > 0) {
        message += "🔹 *다음 작업 예정*\n";
        pendingItems.forEach(row => {
          message += `📦 *품번:* ${row.품번} (${row.품명})\n`;
          message += `🏭 *라인:* ${row.라인} (${row.ITMNO})\n`;
          message += `🏭 *작업순번:* ${row.작업순번}\n`;
          message += `📊 *현재 수량:* ${row.현재수량}\n`;
          message += `📉 *필요 수량:* ${row.필요수량}\n`;
          message += `⚠️ *부족 수량:* ${row.부족수량}\n\n`;
        });
      }

      await sendSlackAlert(message);
    } else {
      console.log("✅ 모든 로뜨인 재고 충분함");
      await sendSlackAlert("✅ *로뜨인 재고 감시 완료: 모든 재고가 충분합니다.*");
    }

  } catch (err) {
    console.error("❌ Error fetching data:", err);
    await sendSlackAlert("❌ *로뜨인 재고 감시 오류 발생!*");
  }

  // 다음 실행 시간 계산 (현재 시간 + 1시간)
  const nextRunTime = moment().add(1, 'hour').format("HH:mm");

  // 감시 종료 및 다음 실행 예고 메시지 전송
  await sendSlackAlert(`✅ *로뜨인 재고 감시 완료!* 다음 감시는 *${nextRunTime}* 에 시작됩니다. 🕒`);
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

// 프로그램 종료 감지 (Ctrl + C)
process.on('SIGINT', async () => {
  console.log("\n🛑 감시 프로세스 종료 중...");
  await sendSlackAlert("🔴 *로뜨인 재고 감시 프로그램 종료!*");

  if (pool) {
    console.log("🔌 데이터베이스 연결 해제...");
    await pool.close();
  }

  console.log("👋 프로그램 종료 완료.");
  process.exit(0);
});

// 프로그램 시작 시 Slack 알림
(async () => {
  console.log("🚀 로뜨인 재고 감시 프로그램 시작!");
  await sendSlackAlert("🟢 *로뜨인 재고 감시 프로그램 시작!*");
  checkInventory();
})();

// 1시간마다 실행
setInterval(checkInventory, 60 * 60 * 1000);
