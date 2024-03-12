exports.queryPosisiHutang = async (tanggal) => { 

    var sql = `
    SELECT MSup.KdMSup, MSup.NmMSup, MSup.Aktif
    , TablePosHut.PosHut, cabang.NmMCabang, cabang.IdMCabang
    FROM (
    SELECT IdMSup, Sum(JmlHut) as PosHut, IdMCabang FROM (
    SELECT TglTrans, IdMSup, JmlHut , IdMCabang
    FROM (
    SELECT IdMSup 
        , 0 as JenisTrans 
        , IdTSAHut as IdTrans 
        , BuktiTSAHut as BuktiTrans 
        , concat(concat(Date(TglTSAHut), ' '), Time(TglUpdate)) as TglTrans 
        , JmlHut 
        , 'Saldo Awal' as Keterangan 
        , IdMCabang 
    FROM MGAPTSAHut 
    WHERE JmlHut <> 0 
    UNION ALL 
    SELECT IdMSup 
        , 1 as JenisTrans 
        , IdTBeli as IdTrans 
        , IF(BuktiTBeli = '', BuktiTLPB, BuktiTBeli) as BuktiTrans 
        , concat(concat(Date(TglTBeli), ' '), Time(TglUpdate)) as TglTrans 
        , (Netto - JmlBayarTunai) AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Pembelian ', BuktiTBeli)) as Keterangan 
        , IdMCabang 
    FROM MGAPTBeli 
    WHERE Hapus = 0 
    AND Void = 0 
    AND (Netto - JmlBayarTunai) <> 0 
    AND HapusLPB = 0 AND VoidLPB = 0 
    AND BuktiTBeli <> ''
    UNION ALL 
    SELECT IdMSup 
        , 2 as JenisTrans 
        , IdTRBeli as IdTrans 
        , BuktiTRBeli as BuktiTrans 
        , concat(concat(Date(TglTRBeli), ' '), Time(TglUpdate)) as TglTrans 
        , - (Netto - JmlBayarTunai) AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Retur Pembelian ', BuktiTRBeli)) as Keterangan 
        , IdMCabang 
    FROM MGAPTRBeli 
    WHERE Hapus = 0 
    AND Void = 0 
    AND JenisTRBeli = 0 
    AND (Netto - JmlBayarTunai) <> 0 
    UNION ALL 
    SELECT IdMSup 
        , 3 as JenisTrans 
        , IdTPBeli as IdTrans 
        , BuktiTPBeli as BuktiTrans 
        , concat(concat(Date(TglTPBeli), ' '), Time(TglUpdate)) as TglTrans 
        , - Total AS JmlHut 
        , concat('Potongan Pembelian ', BuktiTPBeli) as Keterangan 
        , IdMCabang 
    FROM MGAPTPBeli 
    WHERE Hapus = 0 
    AND Void = 0 
    AND Total <> 0 
    UNION ALL 
    SELECT IdMSup 
        , 4 as JenisTrans 
        , IdTBHut as IdTrans 
        , BuktiTBHut as BuktiTrans 
        , concat(concat(Date(TglTBHut), ' '), Time(TglUpdate)) as TglTrans 
        , - Total AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Pembayaran Hutang ', BuktiTBHut)) as Keterangan 
        , IdMCabang 
    FROM MGAPTBHut 
    WHERE Hapus = 0 
    AND Void = 0 
    AND Total <> 0 
    UNION ALL 
    SELECT hut.IdMSup
        , 4.1 AS JenisTrans
        , Hut.IdTBHut AS IdTrans
        , BuktiTBHut AS BuktiTrans
        , CONCAT(CONCAT(DATE(Hut.TglTBHut), ' '), TIME(Hut.TglUpdate)) AS tgltrans
        , HutDB.JmlBayar AS JmlHut
        , IF(Hut.Keterangan <> '', Hut.Keterangan, CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')')) AS Keterangan
        , Hut.IdMCabang 
    FROM MGAPTBHUTDB HutDB
        LEFT OUTER JOIN MGAPTBHut Hut ON (hut.idmcabang = hutDB.idmcabang AND hut.idtbhut = hutdb.idtbhut)
        LEFT OUTER JOIN mgapmsup sup ON (sup.idmsup = hut.idmsup)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = HutDB.IdMCabangMRef AND giro.IdMGiro = HutDB.IdMRef)
    WHERE (Hut.Hapus = 0 AND Hut.Void = 0) AND HutDB.JenisMRef = 'G'
    UNION ALL 
    SELECT sup.idmsup
        , 4.2 AS JenisTrans
        , m.idtgirocair AS IdTrans
        , m.BuktiTGiroCair AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.TglTGiroCair), ' '), TIME(m.TglUpdate)) AS TglTrans
        , - HutDB.JmlBayar AS JmlHut
        , IF(m.Keterangan <> '', m.Keterangan, CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')')) AS Keterangan
        , m.IdMCabang 
    FROM MGKBTGiroCairD d
        LEFT OUTER JOIN MGKBTGiroCair m ON (m.IdMCabang = d.IdMCabang AND d.idtgirocair = m.idtgirocair)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = d.idmcabangmgiro AND giro.idmgiro = d.idmgiro)
        LEFT OUTER JOIN MGAPMSup sup ON (sup.idmsup = giro.idmsup AND giro.jenismgiro = 'K')
        LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
        LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND hut.idtbhut = hutDB.idtbhut)
    WHERE (m.Hapus = 0 AND m.Void = 0) AND m.JenisTGiroCair = 'K' AND HUTDB.JenisMRef = 'G' AND HUT.Void = 0 AND HUT.Hapus = 0
    UNION ALL 
    SELECT Sup.idmsup 
        , 4.3 AS JenisTrans 
        , m.idtgirotolak AS IdTrans 
        , m.buktitgirotolak AS BuktiTrans 
        , CONCAT(CONCAT(DATE(m.tgltgirotolak), ' '), TIME(m.tglupdate)) AS TglTrans
        , HutDB.jmlbayar AS JmlHut 
        , IF(m.Keterangan <> '', m.Keterangan, CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')')) AS keterangan
        , m.IdMCabang 
    FROM MGKBTGiroTolakD d
        LEFT OUTER JOIN MGKBTGiroTolak m ON (m.IdMCabang = d.IdMCabang AND m.IdTGiroTolak = d.IdTGiroTolak)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
        LEFT OUTER JOIN MGAPMSup sup ON (sup.IdMSup = giro.IdMSup AND giro.JenisMGiro = 'K')
        LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
        LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
    WHERE (m.Hapus = 0 AND m.Void = 0)
            AND m.JenisTGiroTolak = 'K' AND HutDB.JenisMRef = 'G' 
            AND Hut.Void = 0 AND Hut.Hapus = 0
    UNION ALL 
    SELECT Sup.IdMSup AS IdMSup 
        , 4.4 AS JenisTrans 
        , m.IdTGiroGanti AS IdTrans 
        , m.BuktiTGiroGanti AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.tgltgiroganti), ' '), TIME(m.tglupdate)) AS tgltrans
        , -d.JmlBayar AS JmlHut
        , IF(m.Keterangan <> '', m.Keterangan, CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')')) AS Keterangan
        , m.IdMCabang 
    FROM MGKBTGiroGantiDG d
        LEFT OUTER JOIN MGKBTGiroGanti m ON (m.IdMCabang = d.IdMCabang AND m.IDTGiroGanti = d.IDTGiroGanti)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
        LEFT OUTER JOIN MGAPMSup Sup ON (Sup.IdMSup = giro.IdMSup AND Giro.JenisMGiro = 'K')
        LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = Giro.IdMGiro AND HutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
    WHERE (m.Hapus = 0 AND m.Void = 0)
            AND m.JenisTGiroGanti = 'K' AND HutDB.JenisMRef = 'G'
            AND Hut.Void = 0 AND Hut.Hapus = 0
    UNION ALL 
    SELECT IdMSup 
        , 5 as JenisTrans 
        , IdTKorHut as IdTrans 
        , BuktiTKorHut as BuktiTrans 
        , concat(concat(Date(TglTKorHut), ' '), Time(TglUpdate)) as TglTrans 
        , Total AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Koreksi Hutang ', BuktiTKorHut)) as Keterangan
        , IdMCabang 
    FROM MGAPTKorHut 
    WHERE Hapus = 0 AND Void = 0 
    AND Total <> 0 
    ) Tbl
    UNION ALL
    SELECT '${tanggal} 00:00:00' as TglTrans, IdMSup, 0 as PosHut, 0 as IdMCabang FROM MGAPMSup
    ) TransAll
    WHERE TglTrans <= '${tanggal} 23:59:59'
    GROUP BY IdMSup
    ) TablePosHut 
    LEFT OUTER JOIN MGAPMSup MSup ON (TablePosHut.IdMSup = MSup.IdMSup)
    LEFT OUTER JOIN mgsymcabang cabang ON (TablePosHut.IdMCabang= cabang.IdMCabang)
    WHERE MSup.Hapus = 0
    AND MSup.Aktif = 1
    AND MSup.KdMSup LIKE '%%'
    AND MSup.NmMSup LIKE '%%'
    AND PosHut <> 0
    ORDER BY MSup.NmMSup

    `;

    return sql;
}



exports.queryKartuHutang = async (start, end) => { 
    var sql = `SELECT MSup.KdMSup
        , MSup.NmMSup
        , TableKartuHut.IdMSup
        , Urut
        , BuktiTrans
        , cast(TglTrans As DateTime) as TglTrans
        , TableKartuHut.Keterangan
        , Saldo
        , JmlHut
        , IF(Urut = 0, 0, IF(Coalesce(JmlHut, 0) > 0, Coalesce(JmlHut, 0), 0)) As Debit
        , IF(Urut = 0, 0, IF(Coalesce(JmlHut, 0) >= 0, 0, Coalesce(JmlHut, 0))) As Kredit
    FROM (
    SELECT IdMSup, 0 As Urut, 0 as JenisTrans, 0 as IdTrans, '-' As BuktiTrans, cast('2024-03-01 00:00:00' as DateTime) As TglTrans, 0 As JmlHut, sum(JmlHut) As Saldo, 'Saldo Sebelumnya' As Keterangan FROM (
    SELECT IdMSup, 0 As JmlHut FROM MGAPMSup
    UNION ALL
    SELECT IdMSup, Sum(JmlHut) as JmlHut
    FROM (SELECT IdMSup 
        , 0 as JenisTrans 
        , IdTSAHut as IdTrans 
        , BuktiTSAHut as BuktiTrans 
        , concat(concat(Date(TglTSAHut), ' '), Time(TglUpdate)) as TglTrans 
        , JmlHut 
        , 'Saldo Awal' as Keterangan 
    FROM MGAPTSAHut 
    WHERE JmlHut <> 0 
    UNION ALL 
    SELECT IdMSup 
        , 1 as JenisTrans 
        , IdTBeli as IdTrans 
        , IF(BuktiTBeli = '', BuktiTLPB, BuktiTBeli) as BuktiTrans 
        , concat(concat(Date(TglTBeli), ' '), Time(TglUpdate)) as TglTrans 
        , (Netto - JmlBayarTunai) AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Pembelian ', BuktiTBeli)) as Keterangan 
    FROM MGAPTBeli 
    WHERE Hapus = 0 
    AND Void = 0 
    AND (Netto - JmlBayarTunai) <> 0 
    AND HapusLPB = 0 AND VoidLPB = 0 
    AND BuktiTBeli <> ''
    UNION ALL 
    SELECT IdMSup 
        , 2 as JenisTrans 
        , IdTRBeli as IdTrans 
        , BuktiTRBeli as BuktiTrans 
        , concat(concat(Date(TglTRBeli), ' '), Time(TglUpdate)) as TglTrans 
        , - (Netto - JmlBayarTunai) AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Retur Pembelian ', BuktiTRBeli)) as Keterangan 
    FROM MGAPTRBeli 
    WHERE Hapus = 0 
    AND Void = 0 
    AND JenisTRBeli = 0 
    AND (Netto - JmlBayarTunai) <> 0 
    UNION ALL 
    SELECT IdMSup 
        , 3 as JenisTrans 
        , IdTPBeli as IdTrans 
        , BuktiTPBeli as BuktiTrans 
        , concat(concat(Date(TglTPBeli), ' '), Time(TglUpdate)) as TglTrans 
        , - Total AS JmlHut 
        , concat('Potongan Pembelian ', BuktiTPBeli) as Keterangan 
    FROM MGAPTPBeli 
    WHERE Hapus = 0 
    AND Void = 0 
    AND Total <> 0 
    UNION ALL 
    SELECT IdMSup 
        , 4 as JenisTrans 
        , IdTBHut as IdTrans 
        , BuktiTBHut as BuktiTrans 
        , concat(concat(Date(TglTBHut), ' '), Time(TglUpdate)) as TglTrans 
        , - Total AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Pembayaran Hutang ', BuktiTBHut)) as Keterangan 
    FROM MGAPTBHut 
    WHERE Hapus = 0 
    AND Void = 0 
    AND Total <> 0 
    UNION ALL 
    SELECT hut.IdMSup
        , 4.1 AS JenisTrans
        , Hut.IdTBHut AS IdTrans
        , BuktiTBHut AS BuktiTrans
        , CONCAT(CONCAT(DATE(Hut.TglTBHut), ' '), TIME(Hut.TglUpdate)) AS tgltrans
        , HutDB.JmlBayar AS JmlHut
        , IF(Hut.Keterangan <> '', Hut.Keterangan, CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')')) AS Keterangan
    FROM MGAPTBHUTDB HutDB
        LEFT OUTER JOIN MGAPTBHut Hut ON (hut.idmcabang = hutDB.idmcabang AND hut.idtbhut = hutdb.idtbhut)
        LEFT OUTER JOIN mgapmsup sup ON (sup.idmsup = hut.idmsup)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = HutDB.IdMCabangMRef AND giro.IdMGiro = HutDB.IdMRef)
    WHERE (Hut.Hapus = 0 AND Hut.Void = 0) AND HutDB.JenisMRef = 'G'
    UNION ALL 
    SELECT sup.idmsup
        , 4.2 AS JenisTrans
        , m.idtgirocair AS IdTrans
        , m.BuktiTGiroCair AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.TglTGiroCair), ' '), TIME(m.TglUpdate)) AS TglTrans
        , - HutDB.JmlBayar AS JmlHut
        , IF(m.Keterangan <> '', m.Keterangan, CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')')) AS Keterangan
    FROM MGKBTGiroCairD d
        LEFT OUTER JOIN MGKBTGiroCair m ON (m.IdMCabang = d.IdMCabang AND d.idtgirocair = m.idtgirocair)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = d.idmcabangmgiro AND giro.idmgiro = d.idmgiro)
        LEFT OUTER JOIN MGAPMSup sup ON (sup.idmsup = giro.idmsup AND giro.jenismgiro = 'K')
        LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
        LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND hut.idtbhut = hutDB.idtbhut)
    WHERE (m.Hapus = 0 AND m.Void = 0) AND m.JenisTGiroCair = 'K' AND HUTDB.JenisMRef = 'G' AND HUT.Void = 0 AND HUT.Hapus = 0
    UNION ALL 
    SELECT Sup.idmsup 
        , 4.3 AS JenisTrans 
        , m.idtgirotolak AS IdTrans 
        , m.buktitgirotolak AS BuktiTrans 
        , CONCAT(CONCAT(DATE(m.tgltgirotolak), ' '), TIME(m.tglupdate)) AS TglTrans
        , HutDB.jmlbayar AS JmlHut 
        , IF(m.Keterangan <> '', m.Keterangan, CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')')) AS keterangan
    FROM MGKBTGiroTolakD d
        LEFT OUTER JOIN MGKBTGiroTolak m ON (m.IdMCabang = d.IdMCabang AND m.IdTGiroTolak = d.IdTGiroTolak)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
        LEFT OUTER JOIN MGAPMSup sup ON (sup.IdMSup = giro.IdMSup AND giro.JenisMGiro = 'K')
        LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
        LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
    WHERE (m.Hapus = 0 AND m.Void = 0)
        AND m.JenisTGiroTolak = 'K' AND HutDB.JenisMRef = 'G' 
        AND Hut.Void = 0 AND Hut.Hapus = 0
    UNION ALL 
    SELECT Sup.IdMSup AS IdMSup 
        , 4.4 AS JenisTrans 
        , m.IdTGiroGanti AS IdTrans 
        , m.BuktiTGiroGanti AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.tgltgiroganti), ' '), TIME(m.tglupdate)) AS tgltrans
        , -d.JmlBayar AS JmlHut
        , IF(m.Keterangan <> '', m.Keterangan, CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')')) AS Keterangan
    FROM MGKBTGiroGantiDG d
        LEFT OUTER JOIN MGKBTGiroGanti m ON (m.IdMCabang = d.IdMCabang AND m.IDTGiroGanti = d.IDTGiroGanti)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
        LEFT OUTER JOIN MGAPMSup Sup ON (Sup.IdMSup = giro.IdMSup AND Giro.JenisMGiro = 'K')
        LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = Giro.IdMGiro AND HutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
    WHERE (m.Hapus = 0 AND m.Void = 0)
        AND m.JenisTGiroGanti = 'K' AND HutDB.JenisMRef = 'G'
        AND Hut.Void = 0 AND Hut.Hapus = 0
    UNION ALL 
    SELECT IdMSup 
        , 5 as JenisTrans 
        , IdTKorHut as IdTrans 
        , BuktiTKorHut as BuktiTrans 
        , concat(concat(Date(TglTKorHut), ' '), Time(TglUpdate)) as TglTrans 
        , Total AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Koreksi Hutang ', BuktiTKorHut)) as Keterangan 
    FROM MGAPTKorHut 
    WHERE Hapus = 0 AND Void = 0 
    AND Total <> 0 
    )
    as LKartuHut WHERE TglTrans < '2024-03-01 00:00:00' GROUP BY IdMSup
    ) TableSaldoAwal
    GROUP BY IdMSup
    UNION ALL
    SELECT IdMSup, 1 as Urut, JenisTrans, IdTrans, BuktiTrans, TglTrans, JmlHut, 0 As Saldo, Keterangan 
    FROM (SELECT IdMSup 
        , 0 as JenisTrans 
        , IdTSAHut as IdTrans 
        , BuktiTSAHut as BuktiTrans 
        , concat(concat(Date(TglTSAHut), ' '), Time(TglUpdate)) as TglTrans 
        , JmlHut 
        , 'Saldo Awal' as Keterangan 
    FROM MGAPTSAHut 
    WHERE JmlHut <> 0 
    UNION ALL 
    SELECT IdMSup 
        , 1 as JenisTrans 
        , IdTBeli as IdTrans 
        , IF(BuktiTBeli = '', BuktiTLPB, BuktiTBeli) as BuktiTrans 
        , concat(concat(Date(TglTBeli), ' '), Time(TglUpdate)) as TglTrans 
        , (Netto - JmlBayarTunai) AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Pembelian ', BuktiTBeli)) as Keterangan 
    FROM MGAPTBeli 
    WHERE Hapus = 0 
    AND Void = 0 
    AND (Netto - JmlBayarTunai) <> 0 
    AND HapusLPB = 0 AND VoidLPB = 0 
    AND BuktiTBeli <> ''
    UNION ALL 
    SELECT IdMSup 
        , 2 as JenisTrans 
        , IdTRBeli as IdTrans 
        , BuktiTRBeli as BuktiTrans 
        , concat(concat(Date(TglTRBeli), ' '), Time(TglUpdate)) as TglTrans 
        , - (Netto - JmlBayarTunai) AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Retur Pembelian ', BuktiTRBeli)) as Keterangan 
    FROM MGAPTRBeli 
    WHERE Hapus = 0 
    AND Void = 0 
    AND JenisTRBeli = 0 
    AND (Netto - JmlBayarTunai) <> 0 
    UNION ALL 
    SELECT IdMSup 
        , 3 as JenisTrans 
        , IdTPBeli as IdTrans 
        , BuktiTPBeli as BuktiTrans 
        , concat(concat(Date(TglTPBeli), ' '), Time(TglUpdate)) as TglTrans 
        , - Total AS JmlHut 
        , concat('Potongan Pembelian ', BuktiTPBeli) as Keterangan 
    FROM MGAPTPBeli 
    WHERE Hapus = 0 
    AND Void = 0 
    AND Total <> 0 
    UNION ALL 
    SELECT IdMSup 
        , 4 as JenisTrans 
        , IdTBHut as IdTrans 
        , BuktiTBHut as BuktiTrans 
        , concat(concat(Date(TglTBHut), ' '), Time(TglUpdate)) as TglTrans 
        , - Total AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Pembayaran Hutang ', BuktiTBHut)) as Keterangan 
    FROM MGAPTBHut 
    WHERE Hapus = 0 
    AND Void = 0 
    AND Total <> 0 
    UNION ALL 
    SELECT hut.IdMSup
        , 4.1 AS JenisTrans
        , Hut.IdTBHut AS IdTrans
        , BuktiTBHut AS BuktiTrans
        , CONCAT(CONCAT(DATE(Hut.TglTBHut), ' '), TIME(Hut.TglUpdate)) AS tgltrans
        , HutDB.JmlBayar AS JmlHut
        , IF(Hut.Keterangan <> '', Hut.Keterangan, CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')')) AS Keterangan
    FROM MGAPTBHUTDB HutDB
        LEFT OUTER JOIN MGAPTBHut Hut ON (hut.idmcabang = hutDB.idmcabang AND hut.idtbhut = hutdb.idtbhut)
        LEFT OUTER JOIN mgapmsup sup ON (sup.idmsup = hut.idmsup)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = HutDB.IdMCabangMRef AND giro.IdMGiro = HutDB.IdMRef)
    WHERE (Hut.Hapus = 0 AND Hut.Void = 0) AND HutDB.JenisMRef = 'G'
    UNION ALL 
    SELECT sup.idmsup
        , 4.2 AS JenisTrans
        , m.idtgirocair AS IdTrans
        , m.BuktiTGiroCair AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.TglTGiroCair), ' '), TIME(m.TglUpdate)) AS TglTrans
        , - HutDB.JmlBayar AS JmlHut
        , IF(m.Keterangan <> '', m.Keterangan, CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')')) AS Keterangan
    FROM MGKBTGiroCairD d
        LEFT OUTER JOIN MGKBTGiroCair m ON (m.IdMCabang = d.IdMCabang AND d.idtgirocair = m.idtgirocair)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = d.idmcabangmgiro AND giro.idmgiro = d.idmgiro)
        LEFT OUTER JOIN MGAPMSup sup ON (sup.idmsup = giro.idmsup AND giro.jenismgiro = 'K')
        LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
        LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND hut.idtbhut = hutDB.idtbhut)
    WHERE (m.Hapus = 0 AND m.Void = 0) AND m.JenisTGiroCair = 'K' AND HUTDB.JenisMRef = 'G' AND HUT.Void = 0 AND HUT.Hapus = 0
    UNION ALL 
    SELECT Sup.idmsup 
        , 4.3 AS JenisTrans 
        , m.idtgirotolak AS IdTrans 
        , m.buktitgirotolak AS BuktiTrans 
        , CONCAT(CONCAT(DATE(m.tgltgirotolak), ' '), TIME(m.tglupdate)) AS TglTrans
        , HutDB.jmlbayar AS JmlHut 
        , IF(m.Keterangan <> '', m.Keterangan, CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')')) AS keterangan
    FROM MGKBTGiroTolakD d
        LEFT OUTER JOIN MGKBTGiroTolak m ON (m.IdMCabang = d.IdMCabang AND m.IdTGiroTolak = d.IdTGiroTolak)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
        LEFT OUTER JOIN MGAPMSup sup ON (sup.IdMSup = giro.IdMSup AND giro.JenisMGiro = 'K')
        LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
        LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
    WHERE (m.Hapus = 0 AND m.Void = 0)
        AND m.JenisTGiroTolak = 'K' AND HutDB.JenisMRef = 'G' 
        AND Hut.Void = 0 AND Hut.Hapus = 0
    UNION ALL 
    SELECT Sup.IdMSup AS IdMSup 
        , 4.4 AS JenisTrans 
        , m.IdTGiroGanti AS IdTrans 
        , m.BuktiTGiroGanti AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.tgltgiroganti), ' '), TIME(m.tglupdate)) AS tgltrans
        , -d.JmlBayar AS JmlHut
        , IF(m.Keterangan <> '', m.Keterangan, CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')')) AS Keterangan
    FROM MGKBTGiroGantiDG d
        LEFT OUTER JOIN MGKBTGiroGanti m ON (m.IdMCabang = d.IdMCabang AND m.IDTGiroGanti = d.IDTGiroGanti)
        LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
        LEFT OUTER JOIN MGAPMSup Sup ON (Sup.IdMSup = giro.IdMSup AND Giro.JenisMGiro = 'K')
        LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = Giro.IdMGiro AND HutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
    WHERE (m.Hapus = 0 AND m.Void = 0)
        AND m.JenisTGiroGanti = 'K' AND HutDB.JenisMRef = 'G'
        AND Hut.Void = 0 AND Hut.Hapus = 0
    UNION ALL 
    SELECT IdMSup 
        , 5 as JenisTrans 
        , IdTKorHut as IdTrans 
        , BuktiTKorHut as BuktiTrans 
        , concat(concat(Date(TglTKorHut), ' '), Time(TglUpdate)) as TglTrans 
        , Total AS JmlHut 
        , IF(Keterangan <> '', Keterangan, concat('Koreksi Hutang ', BuktiTKorHut)) as Keterangan 
    FROM MGAPTKorHut 
    WHERE Hapus = 0 AND Void = 0 
    AND Total <> 0 
    ) 
    as LKartuHut WHERE TglTrans >= '${start} 00:00:00' AND TglTrans < '${end} 23:59:59'
    ) TableKartuHut LEFT OUTER JOIN MGAPMSup MSup ON (TableKartuHut.IdMSup = MSup.IdMSup)
    WHERE MSup.Hapus = 0
    AND MSup.KdMSup LIKE '%%'
    AND MSup.NmMSup LIKE '%%'
    ORDER BY MSup.KdMSup, MSup.NmMSup, Urut, TglTrans, JenisTrans, IdTrans
`;

    return sql;
}