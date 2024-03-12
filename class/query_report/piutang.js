exports.queryPosisiPiutang = async (tanggal) => { 
    var sql = `
    SELECT MCabang.KdMCabang, MCabang.NmMCabang, MCabang.Aktif as AktifMCabang
        , MCust.KdMCust, MCust.NmMCust, MCust.Aktif as AktifMCust, MCust.LimitPiut
        , TablePosPiut.PosPiut, (MCust.LimitPiut - TablePosPiut.PosPiut) As Selisih
    FROM (
    SELECT TransAll.IdMCabang, TransAll.IdMCabangMCust, IdMCust, Sum(JmlPiut) as PosPiut 
    FROM (SELECT tbl.TglTrans, tbl.IdMCabang, tbl.IdMCabangMCust, tbl.IdMCust, tbl.JmlPiut
    FROM (
    SELECT IdMCabang as IdMCabangMCust 
        , IdMCust 
        , 0 as JenisTrans 
        , IdMCabang 
        , IdTSAPiut as IdTrans 
        , BuktiTSAPiut as BuktiTrans 
        , concat(concat(Date(TglTSAPiut), ' '), Time(TglTSAPiut)) as TglTrans 
        , 0 as JenisInvoice 
        , JmlPiut 
        , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan 
    FROM MGARTSAPiut 
    WHERE JmlPiut <> 0 
    UNION ALL 
    SELECT IdMCabangMCust 
        , IdMCust 
        , 1 as JenisTrans 
        , IdMCabang 
        , IdTJualPOS as IdTrans 
        , BuktiTJualPOS as BuktiTrans 
        , concat(concat(Date(TglTJualPOS), ' '), Time(TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut 
        , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan 
    FROM MGARTJualPOS 
    WHERE Hapus = 0 AND Void = 0 
    AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0 

    UNION ALL 
    SELECT IdMCabangMCust 
        , IdMCust 
        , 2 as JenisTrans 
        , IdMCabang 
        , IdTJual as IdTrans 
        , BuktiTJual as BuktiTrans 
        , concat(concat(Date(TglTJual), ' '), Time(TglUpdate)) as TglTrans 
        , JenisTJual as JenisInvoice 
        , (JmlBayarKredit) AS JmlPiut 
        , concat('Penjualan ', BuktiTJual) as Keterangan 
    FROM MGARTJual 
    WHERE Hapus = 0 AND Void = 0 
    AND (JmlBayarKredit) <> 0 
    AND Coalesce(IdTRJual, 0) = 0

    UNION ALL 
    SELECT rj.IdMCabangMCust 
        , rj.IdMCust 
        , 3 as JenisTrans 
        , rj.IdMCabang 
        , rj.IdTRJual as IdTrans 
        , rj.BuktiTRJual as BuktiTrans 
        , concat(concat(Date(rj.TglTRJual), ' '), Time(rj.TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , - rj.JmlBayarKredit AS JmlPiut 
        , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan 
    FROM MGARTRJual rj
        LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
    WHERE rj.Hapus = 0 AND rj.Void = 0 
    AND rj.JmlBayarKredit <> 0 
    AND rj.JenisRJual = 0
    AND jual.IdTJual IS NULL

    UNION ALL 
    SELECT TJualLain.IdMCabangCust as IdMCabangMCust 
        , TJualLain.IdMCust as IdMCust 
        , 2.1 as JenisTrans 
        , TJualLain.IdMCabang 
        , TJualLain.IdTJualLain as IdTrans 
        , TJualLain.BuktiAsli as BuktiTrans 
        , concat(concat(Date(TJualLain.TglTJualLain), ' '), Time(TJualLain.TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , TJualLain.Netto AS JmlPiut 
        , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
            IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
            , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
    FROM MGARTJualLain TJualLain
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
    WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0 
    AND TJualLain.Netto <> 0 

    UNION ALL 
    SELECT m.IdMCabangMCust 
        , m.IdMCust 
        , 4 as JenisTrans 
        , m.IdMCabang 
        , m.IdTBPiut as IdTrans 
        , m.BuktiTBPiut as BuktiTrans 
        , concat(concat(Date(m.TglTBPiut), ' '), Time(m.TglUpdate)) as TglTrans 
        , m.JenisInvoice as JenisInvoice 
        , - (d.JmlBayar) AS JmlPiut  
        , CONCAT('Pembayaran Nota '
                ,  IF(d.IdTrans = 0, d.BuktiTrans,
                    IF(d.JenisTrans = 'S', TSAPiut.BuktiTSAPiut,
                    IF(d.JenisTrans = 'T', TJualPOS.BuktiTJualPOS,
                    IF(d.JenisTrans = 'J', TJual.BuktiTJual,
                    IF(d.JenisTrans = 'R', TRJual.BuktiTRJual,
                    IF(d.JenisTrans = 'L', TKorPiut.BuktiTKorPiut,
                    IF(d.JenisTrans = 'D', TJualDenda.BuktiTJual,
                    IF(d.JenisTrans = 'W', TJualLain.BuktiAsli, '')))))))), ' (', (m.BuktiTBPiut), ') ') AS Keterangan
    FROM MGARTBPiutD d 
        LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
        LEFT OUTER JOIN MGSYMCabang MCabang ON (d.IdMCabangTrans = MCabang.IdMCabang)
        LEFT OUTER JOIN MGARTJualPOS TJualPOS ON (d.JenisTrans = 'T' AND d.IdMCabangTrans = TJualPOS.IdMCabang AND d.IdTrans = TJualPOS.IdTJualPOS)
        LEFT OUTER JOIN MGARTJual TJual ON (d.JenisTrans = 'J' AND d.IdMCabangTrans = TJual.IdMCabang AND d.IdTrans = TJual.IdTJual)
        LEFT OUTER JOIN MGARTJual TJualDenda ON (d.JenisTrans = 'D' AND d.IdMCabangTrans = TJualDenda.IdMCabang AND d.IdTrans = TJualDenda.IdTJual)
        LEFT OUTER JOIN MGARTRJual TRJual ON (d.JenisTrans = 'R' AND d.IdMCabangTrans = TRJual.IdMCabang AND d.IdTrans = TRJual.IdTRJual)
        LEFT OUTER JOIN MGARTJualLain TJualLain ON (d.JenisTrans = 'W' AND d.IdMCabangTrans = TJualLain.IdMCabang AND d.IdTrans = TJualLain.IdTJualLain)
        LEFT OUTER JOIN MGARTSAPiut TSAPiut ON (d.JenisTrans = 'S' AND d.IdMCabangTrans = TSAPiut.IdMCabang AND m.IdMCust = TSAPiut.IdMCust AND d.IdTrans = TSAPiut.IdTSAPiut)
        LEFT OUTER JOIN MGARTKorPiut TKorPiut ON (d.JenisTrans = 'L' AND d.IdMCabangTrans = TKorPiut.IdMCabang AND d.IdTrans = TKorPiut.IdTKorPiut)
    WHERE m.Hapus = 0 AND m.Void = 0
    AND m.Total <> 0 

    UNION ALL 
    SELECT MCust.IdMCabang
        , MCust.IdMCust AS IdMCust
        , 4.1 AS JenisTrans
        , TBPiut.IdMCabang
        , TBPiut.IdTBPiut AS IdTrans
        , TBPiut.BuktiTBPiut AS BuktiTrans
        , CONCAT(CONCAT(DATE(TBPiutB.TglBayar), ' '), TIME(TBPiut.TglUpdate)) AS TglTrans
        , TBPiut.JenisInvoice as JenisInvoice 
        , TBPiutB.JMLBayar AS JmlPiut
        , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
    FROM MGARTBPiutDB TBPiutB
        LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
    WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'

    UNION ALL 
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 4.2 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroCair AS IdTrans 
        , M.BuktiTGiroCair AS BuktiTrans 
        , CONCAT(CONCAT(DATE(m.TglTGiroCair), ' '), TIME(m.TglUpdate)) AS TglTrans
        , BPiut.JenisInvoice as JenisInvoice 
        , -BPiutDB.JMLBayar AS JmlPiut 
        , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan 
    FROM MGKBTGiroCairD D
        LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0

    UNION ALL 
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 4.3 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroTolak AS IdTrans
        , M.BuktiTGiroTolak AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.TglTGiroTolak), ' '), TIME(m.tglupdate)) AS TglTrans
        , BPiut.JenisInvoice as JenisInvoice 
        , BPiutDB.JMLBayar AS JmlPiut
        , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
    FROM MGKBTGiroTolakD D
        LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    WHERE (M.HAPUS = 0 AND M.VOID = 0) 
            AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G' 
            AND BPiut.VOID = 0 AND BPiut.HAPUS = 0

    UNION ALL 
    SELECT MCust.IdMCabang AS IdMCabang 
        , MCust.IdMCust AS IdMCust 
        , 4.4 AS JenisTrans
        , m.IdMCabang
        , M.IDTGiroGanti AS IdTrans
        , M.BuktiTGiroGanti AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.TglTGiroGanti), ' '), TIME(m.tglupdate)) AS TglTrans
        , BPiut.JenisInvoice as JenisInvoice 
        , -D.JMLBayar AS JmlPiut
        , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
    FROM MGKBTGiroGanti M
        LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    WHERE (M.HAPUS = 0 AND M.VOID = 0)
            AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
            AND BPiut.VOID = 0 AND BPiut.HAPUS = 0

    UNION ALL 
    SELECT IdMCabangMCust 
        , IdMCust 
        , 5 as JenisTrans 
        , IdMCabang 
        , IdTKorPiut as IdTrans 
        , BuktiTKorPiut as BuktiTrans 
        , concat(concat(Date(TglTKorPiut), ' '), Time(TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , Total AS JmlPiut 
        , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan 
    FROM MGARTKorPiut 
    WHERE Hapus = 0 AND Void = 0
    AND Total <> 0 


    ) Tbl

        LEFT OUTER JOIN MGARMJenisInvoice JI on (Tbl.JenisInvoice = JI.IdMJenisInvoice and Tbl.IdMCabang = JI.IdMCabang)
    UNION ALL
    SELECT '${tanggal} 00:00:00' as TglTrans, IdMCabang, IdMCabang as IdMCabangMCust, IdMCust, 0 as JmlPiut 
    FROM MGARMCust
    ) TransAll
    WHERE TglTrans < '${tanggal} 23:59:59'
    GROUP BY TransAll.IdMCabang, TransAll.IdMCabangMCust, IdMCust
    ) TablePosPiut LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosPiut.IdMCabang = MCabang.IdMCabang)
                LEFT OUTER JOIN MGARMCust MCust ON (TablePosPiut.IdMCabangMCust = MCust.IdMCabang AND TablePosPiut.IdMCust = MCust.IdMCust)
    WHERE MCabang.Hapus = 0
    AND MCust.Hapus = 0
    AND MCust.KdMCust LIKE '%%'
    AND MCust.NmMCust LIKE '%%'
    AND PosPiut <> 0
    ORDER BY MCabang.KdMCabang, MCust.NmMCust
    `;

    return sql;
}

exports.queryKartuPiutang = async (start, end) => { 
    var sql = `SELECT MCabang.KdMCabang
        , MCabang.NmMCabang
        , MCust.KdMCust
        , MCust.NmMCust
        , TableKartuPiut.IdMCabangMCust
        , TableKartuPiut.IdMCust
        , TableKartuPiut.IdMCabangTrans
        , Urut
        , BuktiTrans
        , cast(TglTrans as datetime) as TglTrans
        , TableKartuPiut.Keterangan, Saldo, JmlPiut
        , IF(Urut = 0, 0, IF(Coalesce(JmlPiut) > 0, Coalesce(JmlPiut), 0)) As Debit
        , IF(Urut = 0, 0, IF(Coalesce(JmlPiut) >= 0, 0, Coalesce(JmlPiut))) As Kredit
    FROM (
    SELECT IdMCabangTrans, IdMCabangMCust, IdMCust, 0 As Urut, 0 as JenisTrans, 0 as IdTrans, '-' As BuktiTrans, cast('${start} 00:00:00' as DateTime) As TglTrans, 0 As JmlPiut, sum(JmlPiut) As Saldo, 'Saldo Sebelumnya' As Keterangan
    FROM (
    SELECT IdMCabang as IdMCabangTrans, IdMCabang as IdMCabangMCust, IdMCust, 0 As JmlPiut FROM MGARMCust
    UNION ALL
    SELECT LKartuPiut.IdMCabang as IdMCabangTrans, LKartuPiut.IdMCabangMCust, LKartuPiut.IdMCust, Sum(LKartuPiut.JmlPiut) as JmlPiut 
    FROM (
    SELECT IdMCabang as IdMCabangMCust 
        , IdMCust 
        , 0 as JenisTrans 
        , IdMCabang 
        , IdTSAPiut as IdTrans 
        , BuktiTSAPiut as BuktiTrans 
        , concat(concat(Date(TglTSAPiut), ' '), Time(TglTSAPiut)) as TglTrans 
        , 0 as JenisInvoice 
        , JmlPiut 
        , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan 
    FROM MGARTSAPiut 
    WHERE JmlPiut <> 0 
    UNION ALL 
    SELECT IdMCabangMCust 
        , IdMCust 
        , 1 as JenisTrans 
        , IdMCabang 
        , IdTJualPOS as IdTrans 
        , BuktiTJualPOS as BuktiTrans 
        , concat(concat(Date(TglTJualPOS), ' '), Time(TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut 
        , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan 
    FROM MGARTJualPOS 
    WHERE Hapus = 0 AND Void = 0 
    AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0 

    UNION ALL 
    SELECT IdMCabangMCust 
        , IdMCust 
        , 2 as JenisTrans 
        , IdMCabang 
        , IdTJual as IdTrans 
        , BuktiTJual as BuktiTrans 
        , concat(concat(Date(TglTJual), ' '), Time(TglUpdate)) as TglTrans 
        , JenisTJual as JenisInvoice 
        , (JmlBayarKredit) AS JmlPiut 
        , concat('Penjualan ', BuktiTJual) as Keterangan 
    FROM MGARTJual 
    WHERE Hapus = 0 AND Void = 0 
    AND (JmlBayarKredit) <> 0 
    AND Coalesce(IdTRJual, 0) = 0

    UNION ALL 
    SELECT rj.IdMCabangMCust 
        , rj.IdMCust 
        , 3 as JenisTrans 
        , rj.IdMCabang 
        , rj.IdTRJual as IdTrans 
        , rj.BuktiTRJual as BuktiTrans 
        , concat(concat(Date(rj.TglTRJual), ' '), Time(rj.TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , - rj.JmlBayarKredit AS JmlPiut 
        , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan 
    FROM MGARTRJual rj
        LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
    WHERE rj.Hapus = 0 AND rj.Void = 0 
    AND rj.JmlBayarKredit <> 0 
    AND rj.JenisRJual = 0
    AND jual.IdTJual IS NULL

    UNION ALL 
    SELECT TJualLain.IdMCabangCust as IdMCabangMCust 
        , TJualLain.IdMCust as IdMCust 
        , 2.1 as JenisTrans 
        , TJualLain.IdMCabang 
        , TJualLain.IdTJualLain as IdTrans 
        , TJualLain.BuktiAsli as BuktiTrans 
        , concat(concat(Date(TJualLain.TglTJualLain), ' '), Time(TJualLain.TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , TJualLain.Netto AS JmlPiut 
        , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
            IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
            , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
    FROM MGARTJualLain TJualLain
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
    WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0 
    AND TJualLain.Netto <> 0 

    UNION ALL 
    SELECT m.IdMCabangMCust 
        , m.IdMCust 
        , 4 as JenisTrans 
        , m.IdMCabang 
        , m.IdTBPiut as IdTrans 
        , m.BuktiTBPiut as BuktiTrans 
        , concat(concat(Date(m.TglTBPiut), ' '), Time(m.TglUpdate)) as TglTrans 
        , m.JenisInvoice as JenisInvoice 
        , - (d.JmlBayar) AS JmlPiut  
        , CONCAT('Pembayaran Nota '
                ,  IF(d.IdTrans = 0, d.BuktiTrans,
                    IF(d.JenisTrans = 'S', TSAPiut.BuktiTSAPiut,
                    IF(d.JenisTrans = 'T', TJualPOS.BuktiTJualPOS,
                    IF(d.JenisTrans = 'J', TJual.BuktiTJual,
                    IF(d.JenisTrans = 'R', TRJual.BuktiTRJual,
                    IF(d.JenisTrans = 'L', TKorPiut.BuktiTKorPiut,
                    IF(d.JenisTrans = 'D', TJualDenda.BuktiTJual,
                    IF(d.JenisTrans = 'W', TJualLain.BuktiAsli, '')))))))), ' (', (m.BuktiTBPiut), ') ') AS Keterangan
    FROM MGARTBPiutD d 
        LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
        LEFT OUTER JOIN MGSYMCabang MCabang ON (d.IdMCabangTrans = MCabang.IdMCabang)
        LEFT OUTER JOIN MGARTJualPOS TJualPOS ON (d.JenisTrans = 'T' AND d.IdMCabangTrans = TJualPOS.IdMCabang AND d.IdTrans = TJualPOS.IdTJualPOS)
        LEFT OUTER JOIN MGARTJual TJual ON (d.JenisTrans = 'J' AND d.IdMCabangTrans = TJual.IdMCabang AND d.IdTrans = TJual.IdTJual)
        LEFT OUTER JOIN MGARTJual TJualDenda ON (d.JenisTrans = 'D' AND d.IdMCabangTrans = TJualDenda.IdMCabang AND d.IdTrans = TJualDenda.IdTJual)
        LEFT OUTER JOIN MGARTRJual TRJual ON (d.JenisTrans = 'R' AND d.IdMCabangTrans = TRJual.IdMCabang AND d.IdTrans = TRJual.IdTRJual)
        LEFT OUTER JOIN MGARTJualLain TJualLain ON (d.JenisTrans = 'W' AND d.IdMCabangTrans = TJualLain.IdMCabang AND d.IdTrans = TJualLain.IdTJualLain)
        LEFT OUTER JOIN MGARTSAPiut TSAPiut ON (d.JenisTrans = 'S' AND d.IdMCabangTrans = TSAPiut.IdMCabang AND m.IdMCust = TSAPiut.IdMCust AND d.IdTrans = TSAPiut.IdTSAPiut)
        LEFT OUTER JOIN MGARTKorPiut TKorPiut ON (d.JenisTrans = 'L' AND d.IdMCabangTrans = TKorPiut.IdMCabang AND d.IdTrans = TKorPiut.IdTKorPiut)
    WHERE m.Hapus = 0 AND m.Void = 0
    AND m.Total <> 0 

    UNION ALL 
    SELECT MCust.IdMCabang
        , MCust.IdMCust AS IdMCust
        , 4.1 AS JenisTrans
        , TBPiut.IdMCabang
        , TBPiut.IdTBPiut AS IdTrans
        , TBPiut.BuktiTBPiut AS BuktiTrans
        , CONCAT(CONCAT(DATE(TBPiutB.TglBayar), ' '), TIME(TBPiut.TglUpdate)) AS TglTrans
        , TBPiut.JenisInvoice as JenisInvoice 
        , TBPiutB.JMLBayar AS JmlPiut
        , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
    FROM MGARTBPiutDB TBPiutB
        LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
    WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'

    UNION ALL 
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 4.2 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroCair AS IdTrans 
        , M.BuktiTGiroCair AS BuktiTrans 
        , CONCAT(CONCAT(DATE(m.TglTGiroCair), ' '), TIME(m.TglUpdate)) AS TglTrans
        , BPiut.JenisInvoice as JenisInvoice 
        , -BPiutDB.JMLBayar AS JmlPiut 
        , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan 
    FROM MGKBTGiroCairD D
        LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0

    UNION ALL 
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 4.3 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroTolak AS IdTrans
        , M.BuktiTGiroTolak AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.TglTGiroTolak), ' '), TIME(m.tglupdate)) AS TglTrans
        , BPiut.JenisInvoice as JenisInvoice 
        , BPiutDB.JMLBayar AS JmlPiut
        , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
    FROM MGKBTGiroTolakD D
        LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    WHERE (M.HAPUS = 0 AND M.VOID = 0) 
        AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G' 
        AND BPiut.VOID = 0 AND BPiut.HAPUS = 0

    UNION ALL 
    SELECT MCust.IdMCabang AS IdMCabang 
        , MCust.IdMCust AS IdMCust 
        , 4.4 AS JenisTrans
        , m.IdMCabang
        , M.IDTGiroGanti AS IdTrans
        , M.BuktiTGiroGanti AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.TglTGiroGanti), ' '), TIME(m.tglupdate)) AS TglTrans
        , BPiut.JenisInvoice as JenisInvoice 
        , -D.JMLBayar AS JmlPiut
        , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
    FROM MGKBTGiroGanti M
        LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    WHERE (M.HAPUS = 0 AND M.VOID = 0)
        AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
        AND BPiut.VOID = 0 AND BPiut.HAPUS = 0

    UNION ALL 
    SELECT IdMCabangMCust 
        , IdMCust 
        , 5 as JenisTrans 
        , IdMCabang 
        , IdTKorPiut as IdTrans 
        , BuktiTKorPiut as BuktiTrans 
        , concat(concat(Date(TglTKorPiut), ' '), Time(TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , Total AS JmlPiut 
        , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan 
    FROM MGARTKorPiut 
    WHERE Hapus = 0 AND Void = 0
    AND Total <> 0 


    ) LKartuPiut
    left outer join mgarmjenisinvoice m on (lkartupiut.jenisinvoice = m.idmjenisinvoice)
    WHERE TglTrans < '${start} 00:00:00'
    GROUP BY LKartuPiut.IdMCabang, LKartuPiut.IdMCabangMCust, LKartuPiut.IdMCust
    ) TableSaldoAwal
    GROUP BY IdMCabangTrans, IdMCabangMCust, IdMCust
    UNION ALL
    SELECT LKartuPiut.IdMCabang as IdMCabangTrans, LKartuPiut.IdMCabangMCust, LKartuPiut.IdMCust, 1 as Urut, LKartuPiut.JenisTrans, LKartuPiut.IdTrans, LKartuPiut.BuktiTrans, LKartuPiut.TglTrans, LKartuPiut.JmlPiut, 0, LKartuPiut.Keterangan
    FROM (
    SELECT IdMCabang as IdMCabangMCust 
        , IdMCust 
        , 0 as JenisTrans 
        , IdMCabang 
        , IdTSAPiut as IdTrans 
        , BuktiTSAPiut as BuktiTrans 
        , concat(concat(Date(TglTSAPiut), ' '), Time(TglTSAPiut)) as TglTrans 
        , 0 as JenisInvoice 
        , JmlPiut 
        , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan 
    FROM MGARTSAPiut 
    WHERE JmlPiut <> 0 
    UNION ALL 
    SELECT IdMCabangMCust 
        , IdMCust 
        , 1 as JenisTrans 
        , IdMCabang 
        , IdTJualPOS as IdTrans 
        , BuktiTJualPOS as BuktiTrans 
        , concat(concat(Date(TglTJualPOS), ' '), Time(TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut 
        , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan 
    FROM MGARTJualPOS 
    WHERE Hapus = 0 AND Void = 0 
    AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0 

    UNION ALL 
    SELECT IdMCabangMCust 
        , IdMCust 
        , 2 as JenisTrans 
        , IdMCabang 
        , IdTJual as IdTrans 
        , BuktiTJual as BuktiTrans 
        , concat(concat(Date(TglTJual), ' '), Time(TglUpdate)) as TglTrans 
        , JenisTJual as JenisInvoice 
        , (JmlBayarKredit) AS JmlPiut 
        , concat('Penjualan ', BuktiTJual) as Keterangan 
    FROM MGARTJual 
    WHERE Hapus = 0 AND Void = 0 
    AND (JmlBayarKredit) <> 0 
    AND Coalesce(IdTRJual, 0) = 0

    UNION ALL 
    SELECT rj.IdMCabangMCust 
        , rj.IdMCust 
        , 3 as JenisTrans 
        , rj.IdMCabang 
        , rj.IdTRJual as IdTrans 
        , rj.BuktiTRJual as BuktiTrans 
        , concat(concat(Date(rj.TglTRJual), ' '), Time(rj.TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , - rj.JmlBayarKredit AS JmlPiut 
        , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan 
    FROM MGARTRJual rj
        LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
    WHERE rj.Hapus = 0 AND rj.Void = 0 
    AND rj.JmlBayarKredit <> 0 
    AND rj.JenisRJual = 0
    AND jual.IdTJual IS NULL

    UNION ALL 
    SELECT TJualLain.IdMCabangCust as IdMCabangMCust 
        , TJualLain.IdMCust as IdMCust 
        , 2.1 as JenisTrans 
        , TJualLain.IdMCabang 
        , TJualLain.IdTJualLain as IdTrans 
        , TJualLain.BuktiAsli as BuktiTrans 
        , concat(concat(Date(TJualLain.TglTJualLain), ' '), Time(TJualLain.TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , TJualLain.Netto AS JmlPiut 
        , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
            IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
            , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
    FROM MGARTJualLain TJualLain
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
    WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0 
    AND TJualLain.Netto <> 0 

    UNION ALL 
    SELECT m.IdMCabangMCust 
        , m.IdMCust 
        , 4 as JenisTrans 
        , m.IdMCabang 
        , m.IdTBPiut as IdTrans 
        , m.BuktiTBPiut as BuktiTrans 
        , concat(concat(Date(m.TglTBPiut), ' '), Time(m.TglUpdate)) as TglTrans 
        , m.JenisInvoice as JenisInvoice 
        , - (d.JmlBayar) AS JmlPiut  
        , CONCAT('Pembayaran Nota '
                ,  IF(d.IdTrans = 0, d.BuktiTrans,
                    IF(d.JenisTrans = 'S', TSAPiut.BuktiTSAPiut,
                    IF(d.JenisTrans = 'T', TJualPOS.BuktiTJualPOS,
                    IF(d.JenisTrans = 'J', TJual.BuktiTJual,
                    IF(d.JenisTrans = 'R', TRJual.BuktiTRJual,
                    IF(d.JenisTrans = 'L', TKorPiut.BuktiTKorPiut,
                    IF(d.JenisTrans = 'D', TJualDenda.BuktiTJual,
                    IF(d.JenisTrans = 'W', TJualLain.BuktiAsli, '')))))))), ' (', (m.BuktiTBPiut), ') ') AS Keterangan
    FROM MGARTBPiutD d 
        LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
        LEFT OUTER JOIN MGSYMCabang MCabang ON (d.IdMCabangTrans = MCabang.IdMCabang)
        LEFT OUTER JOIN MGARTJualPOS TJualPOS ON (d.JenisTrans = 'T' AND d.IdMCabangTrans = TJualPOS.IdMCabang AND d.IdTrans = TJualPOS.IdTJualPOS)
        LEFT OUTER JOIN MGARTJual TJual ON (d.JenisTrans = 'J' AND d.IdMCabangTrans = TJual.IdMCabang AND d.IdTrans = TJual.IdTJual)
        LEFT OUTER JOIN MGARTJual TJualDenda ON (d.JenisTrans = 'D' AND d.IdMCabangTrans = TJualDenda.IdMCabang AND d.IdTrans = TJualDenda.IdTJual)
        LEFT OUTER JOIN MGARTRJual TRJual ON (d.JenisTrans = 'R' AND d.IdMCabangTrans = TRJual.IdMCabang AND d.IdTrans = TRJual.IdTRJual)
        LEFT OUTER JOIN MGARTJualLain TJualLain ON (d.JenisTrans = 'W' AND d.IdMCabangTrans = TJualLain.IdMCabang AND d.IdTrans = TJualLain.IdTJualLain)
        LEFT OUTER JOIN MGARTSAPiut TSAPiut ON (d.JenisTrans = 'S' AND d.IdMCabangTrans = TSAPiut.IdMCabang AND m.IdMCust = TSAPiut.IdMCust AND d.IdTrans = TSAPiut.IdTSAPiut)
        LEFT OUTER JOIN MGARTKorPiut TKorPiut ON (d.JenisTrans = 'L' AND d.IdMCabangTrans = TKorPiut.IdMCabang AND d.IdTrans = TKorPiut.IdTKorPiut)
    WHERE m.Hapus = 0 AND m.Void = 0
    AND m.Total <> 0 

    UNION ALL 
    SELECT MCust.IdMCabang
        , MCust.IdMCust AS IdMCust
        , 4.1 AS JenisTrans
        , TBPiut.IdMCabang
        , TBPiut.IdTBPiut AS IdTrans
        , TBPiut.BuktiTBPiut AS BuktiTrans
        , CONCAT(CONCAT(DATE(TBPiutB.TglBayar), ' '), TIME(TBPiut.TglUpdate)) AS TglTrans
        , TBPiut.JenisInvoice as JenisInvoice 
        , TBPiutB.JMLBayar AS JmlPiut
        , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
    FROM MGARTBPiutDB TBPiutB
        LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
    WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'

    UNION ALL 
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 4.2 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroCair AS IdTrans 
        , M.BuktiTGiroCair AS BuktiTrans 
        , CONCAT(CONCAT(DATE(m.TglTGiroCair), ' '), TIME(m.TglUpdate)) AS TglTrans
        , BPiut.JenisInvoice as JenisInvoice 
        , -BPiutDB.JMLBayar AS JmlPiut 
        , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan 
    FROM MGKBTGiroCairD D
        LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0

    UNION ALL 
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 4.3 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroTolak AS IdTrans
        , M.BuktiTGiroTolak AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.TglTGiroTolak), ' '), TIME(m.tglupdate)) AS TglTrans
        , BPiut.JenisInvoice as JenisInvoice 
        , BPiutDB.JMLBayar AS JmlPiut
        , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
    FROM MGKBTGiroTolakD D
        LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    WHERE (M.HAPUS = 0 AND M.VOID = 0) 
        AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G' 
        AND BPiut.VOID = 0 AND BPiut.HAPUS = 0

    UNION ALL 
    SELECT MCust.IdMCabang AS IdMCabang 
        , MCust.IdMCust AS IdMCust 
        , 4.4 AS JenisTrans
        , m.IdMCabang
        , M.IDTGiroGanti AS IdTrans
        , M.BuktiTGiroGanti AS BuktiTrans
        , CONCAT(CONCAT(DATE(m.TglTGiroGanti), ' '), TIME(m.tglupdate)) AS TglTrans
        , BPiut.JenisInvoice as JenisInvoice 
        , -D.JMLBayar AS JmlPiut
        , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
    FROM MGKBTGiroGanti M
        LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    WHERE (M.HAPUS = 0 AND M.VOID = 0)
        AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
        AND BPiut.VOID = 0 AND BPiut.HAPUS = 0

    UNION ALL 
    SELECT IdMCabangMCust 
        , IdMCust 
        , 5 as JenisTrans 
        , IdMCabang 
        , IdTKorPiut as IdTrans 
        , BuktiTKorPiut as BuktiTrans 
        , concat(concat(Date(TglTKorPiut), ' '), Time(TglUpdate)) as TglTrans 
        , 0 as JenisInvoice 
        , Total AS JmlPiut 
        , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan 
    FROM MGARTKorPiut 
    WHERE Hapus = 0 AND Void = 0
    AND Total <> 0 


    ) LKartuPiut 
    left outer join mgarmjenisinvoice m on (lkartupiut.jenisinvoice = m.idmjenisinvoice)
    where LKartuPiut.TglTrans >= '${start} 00:00:00' and LKartuPiut.TglTrans <= '${end} 23:59:59'
    ) TableKartuPiut LEFT OUTER JOIN MGSYMCabang MCabang ON (TableKartuPiut.IdMCabangTrans = MCabang.IdMCabang)
                    LEFT OUTER JOIN MGARMCust MCust ON (TableKartuPiut.IdMCabangMCust = MCust.IdMCabang AND TableKartuPiut.IdMCust = MCust.IdMCust)
    WHERE MCabang.Hapus = 0
    AND MCabang.Aktif = 1
    AND MCabang.KdMCabang LIKE '%AIH%'
    AND MCabang.NmMCabang LIKE '%ALAM INDAH HARMONI%'
    AND MCust.Hapus = 0
    AND MCust.Aktif = 1
    AND MCust.KdMCust LIKE '%%'
    AND MCust.NmMCust LIKE '%%'
    ORDER BY MCabang.KdMCabang, MCabang.NmMCabang, TableKartuPiut.IdMCabangMCust
        , MCust.KdMCust, MCust.NmMCust, Urut, TglTrans, JenisTrans, IdTrans
`;

    return sql;
}