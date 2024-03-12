const fun = require("../../mgmx");

exports.queryPosisiPiutang = async (companyid,tanggal) => { 
    var sql = ``;
    if (companyid == fun.companyWI) {
        sql = `
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
        AND MCabang.KdMCabang LIKE '%%'
        AND MCabang.NmMCabang LIKE '%%'
        AND MCust.Hapus = 0
        AND MCust.KdMCust LIKE '%%'
        AND MCust.NmMCust LIKE '%%'
        AND PosPiut <> 0
        ORDER BY MCabang.KdMCabang, MCust.NmMCust
        `;
    } else {
        sql = `SELECT MCabang.KdMCabang, MCabang.NmMCabang, MCabang.Aktif as AktifMCabang
        , Coalesce(MCust.KdMCust,'') as KdMCust, Coalesce(MCust.NmMCust,'') as NmMCust, coalesce(MCust.Aktif,1) as AktifMCust, coalesce(MCust.LimitPiut,0) as LimitPiut
        , TablePosPiut.PosPiut, (MCust.LimitPiut - TablePosPiut.PosPiut) As Selisih
        FROM (
            SELECT TransAll.IdMCabang, TransAll.IdMCabangMCust, IdMCust, Sum(JmlPiut) as PosPiut
            FROM (SELECT tbl.TglTrans, tbl.IdMCabang, tbl.IdMCabangMCust, tbl.IdMCust, tbl.JmlPiut, Tbl.id_bcf
        FROM (
            SELECT IdMCabang as IdMCabangMCust 
                , IdMCust 
                , 0 as JenisTrans 
                , IdMCabang 
                , IdTSAPiut as IdTrans 
                , BuktiTSAPiut as BuktiTrans 
                , concat(Date(TglTSAPiut), ' ', Time(TglTSAPiut)) as TglTrans 
                , 0 as JenisInvoice 
                , JmlPiut 
                , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTSAPiut 
            WHERE JmlPiut <> 0 
            AND TglTSAPiut <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT IdMCabangMCust 
                , IdMCust 
                , 1 as JenisTrans 
                , IdMCabang 
                , IdTJualPOS as IdTrans 
                , BuktiTJualPOS as BuktiTrans 
                , concat(Date(TglTJualPOS), ' ', Time(TglUpdate)) as TglTrans 
                , 0 as JenisInvoice 
                , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut 
                , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTJualPOS 
            WHERE Hapus = 0 AND Void = 0 
            AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0 
            AND TglTJualPOS <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT Jual.IdMCabangMCust 
                , Jual.IdMCust 
                , 2 as JenisTrans 
                , Jual.IdMCabang 
                , Jual.IdTJual as IdTrans 
                , Jual.BuktiTJual as BuktiTrans 
                , concat(Date(Jual.TglTJual), ' ', Time(Jual.TglUpdate)) as TglTrans 
                , Jual.JenisTJual as JenisInvoice 
                , (Jual.JmlBayarKredit) AS JmlPiut 
                , concat('Penjualan ', Jual.BuktiTJual) as Keterangan 
                , Jual.Id_bcf
            FROM MGARTJual Jual
            WHERE Jual.Hapus = 0 AND Jual.Void = 0 
            AND (Jual.JmlBayarKredit) <> 0 
            AND Coalesce(Jual.IdTRJual, 0) = 0
            AND Jual.TglTJual <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT rj.IdMCabangMCust 
                , rj.IdMCust 
                , 3 as JenisTrans 
                , rj.IdMCabang 
                , rj.IdTRJual as IdTrans 
                , rj.BuktiTRJual as BuktiTrans 
                , concat(Date(rj.TglTRJual), ' ', Time(rj.TglUpdate)) as TglTrans 
                , rj.JenisInvoice as JenisInvoice 
                , - rj.JmlBayarKredit AS JmlPiut 
                , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTRJual rj
                LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
            WHERE rj.Hapus = 0 AND rj.Void = 0 
            AND rj.JmlBayarKredit <> 0 
            AND rj.JenisRJual = 0
            AND jual.IdTJual IS NULL
            AND TglTRJual <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT TJualLain.IdMCabangCust as IdMCabangMCust 
                , TJualLain.IdMCust as IdMCust 
                , 2.1 as JenisTrans 
                , TJualLain.IdMCabang 
                , TJualLain.IdTJualLain as IdTrans 
                , TJualLain.BuktiTJualLain as BuktiTrans 
                , concat(Date(TJualLain.TglTJualLain), ' ', Time(TJualLain.TglUpdate)) as TglTrans 
                , 0 as JenisInvoice 
                , TJualLain.Netto AS JmlPiut 
                , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
                    IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
                    , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
                , -1 as Id_bcf
            FROM MGARTJualLain TJualLain
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
            WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0 
            AND TJualLain.Netto <> 0 
            AND TglTJualLain <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT TAngkutan.IdMCabangMCust
                , TAngkutan.IdMCust
                , 2.2 AS JenisTrans
                , TAngkutan.IdMCabang
                , TAngkutan.IdTTAngkutan AS IdTrans
                , TAngkutan.BuktiTTAngkutan AS BuktiTrans
                , CONCAT(DATE(TAngkutan.TglTTAngkutan), ' ', TIME(TAngkutan.TglUpdate)) AS TglTrans
                , TAngkutan.JenisTTAngkutan AS JenisInvoice
                , (TAngkutan.JmlBayarKredit) AS JmlPiut
                , CONCAT('Titip Angkutan ', TAngkutan.BuktiTTAngkutan) AS Keterangan
                , -1 as Id_bcf
            FROM MGARTTAngkutan TAngkutan
            WHERE Hapus = 0 AND Void = 0
            AND (TAngkutan.JmlBayarKredit) <> 0
            AND TglTTAngkutan <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT TJualManAset.IdMCabangCust as IdMCabangMCust
                , TJualManAset.IdMCust
                , 2.3 AS JenisTrans
                , TJualManAset.IdMCabang
                , TJualManAset.IdTJualAset AS IdTrans
                , TJualManAset.BuktiTJualAset AS BuktiTrans
                , CONCAT(DATE(TJualManAset.TglTJualAset), ' ', TIME(TJualManAset.TglUpdate)) AS TglTrans
                , TJualManAset.JenisTJualAset AS JenisInvoice
                , (TJualManAset.JmlBayarKredit) AS JmlPiut
                , CONCAT('Penjualan Aset ', TJualManAset.BuktiTJualAset) AS Keterangan
                , -1 as Id_bcf
            FROM MGARTJualAset TJualManAset
            WHERE Hapus = 0 AND Void = 0
            AND (TJualManAset.JmlBayarKredit) <> 0
            AND TglTJualAset <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT Tbl.IdMCabangMCust 
                , Tbl.IdMCust 
                , Tbl.JenisTrans 
                , Tbl.IdMCabang 
                , Tbl.IdTrans 
                , Tbl.BuktiTrans 
                , Tbl.TglTrans 
                , Tbl.JenisInvoice 
                , Tbl.JmlPiut + IF(SUM(UMJualD.JmlBayar) IS NULL, 0, SUM(UMJualD.JmlBayar)) AS JmlPiut 
                , Tbl.Keterangan 
                , Tbl.Id_bcf 
            FROM ( 
            SELECT m.IdMCabangMCust 
                , m.IdMCust 
                , 4 as JenisTrans 
                , m.IdMCabang 
                , m.IdTBPiut as IdTrans 
                , m.BuktiTBPiut as BuktiTrans 
                , concat(Date(d.TglBayar), ' ', Time(m.TglUpdate)) as TglTrans 
                , m.JenisInvoice as JenisInvoice 
                , - sum(d.JmlBayar) AS JmlPiut 
                , concat('Pembayaran Piutang ', BuktiTBPiut, ' ' ,if(d.jenisMref = 'K',Kas.KdMKas, if(d.JenisMRef ='B',Rek.KdMRek, if(d.JenisMref ='G' ,Giro.KdMGiro,IF(d.JenisMRef = 'P',Prk.KdMPrk,''))))) as Keterangan  
                , d.IdMCabangMRef, d.IdMref, d.TglBayar
                , m.Id_bcf
            FROM MGARTBPiutDB d 
                LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
                LEFT OUTER JOIN MGKBMKas Kas ON(Kas.IdMCabang = d.IdMCabangMref AND Kas.IdMKas = d.IdMref and d.JenisMRef ='K')
                LEFT OUTER JOIN MGKBMRek Rek ON(Rek.IdMCabang = d.IdMCabangMref AND Rek.IdMRek = d.IdMref and d.JenisMRef ='B')
                LEFT OUTER JOIN MGKBMGiro Giro ON(Giro.IdMCabang = d.IdMCabangMref AND Giro.IdMGiro = d.IdMref and d.JenisMRef ='G')
                LEFT OUTER JOIN MGGLMPrk Prk ON( Prk.IdMPrk = d.IdMref and d.JenisMRef ='P' and Prk.Periode = 0)
            WHERE m.Hapus = 0 AND m.Void = 0
            AND m.Total <> 0 
            AND d.TglBayar <= '${tanggal} 23:59:59'
            GROUP BY m.IdMCabang, m.IdTBPiut
            , d.IdMCabangMRef, d.IdMRef, d.TglBayar
            ) Tbl LEFT OUTER JOIN MGARTUMJual UMJual ON(Tbl.IdMCabang = UMJual.IdMCabangTBPiut AND Tbl.IdTrans = UMJual.IdTBPiut AND UMJual.Hapus = 0 AND UMJual.Void = 0)
                LEFT OUTER JOIN MGARTUMJualD UMJualD ON(UMJual.IdMCabang = UMJualD.IdMCabang AND UMJual.IdTUMJual = UMJualD.IdTUMJual AND Tbl.IdMCabangMRef = UMJualD.IdMCabangMRef AND Tbl.IdMref = UMJualD.IdMRef AND Tbl.TglBayar = UMJualD.TglBayar)
        GROUP BY Tbl.IdMCabangMCust
                , Tbl.IdMCust
                , Tbl.JenisTrans
                , Tbl.IdMCabang
                , Tbl.IdTrans
                , Tbl.BuktiTrans
                , Tbl.TglTrans
                , Tbl.JenisInvoice
                , Tbl.JmlPiut
                , Tbl.Keterangan
                , Tbl.Id_bcf
            UNION ALL 
            SELECT m.IdMCabangMCust 
                , m.IdMCust 
                , 4 as JenisTrans 
                , m.IdMCabang 
                , m.IdTBPiut as IdTrans 
                , m.BuktiTBPiut as BuktiTrans 
                , concat(Date(m.TglTBPiut), ' ', Time(m.TglUpdate)) as TglTrans 
                , m.JenisInvoice as JenisInvoice 
                , - m.JmlUM AS JmlPiut 
                , concat('Pembayaran Piutang dgn Uang Muka ', BuktiTBPiut) as Keterangan  
                , m.Id_bcf
            FROM MGARTBPiut m
            WHERE m.Hapus = 0 AND m.Void = 0
            AND m.JmlUM <> 0 
            AND TglTBPiut <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT MCust.IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.1 AS JenisTrans
                , TBPiut.IdMCabang
                , TBPiut.IdTBPiut AS IdTrans
                , TBPiut.BuktiTBPiut AS BuktiTrans
                , CONCAT(DATE(TBPiutB.TglBayar), ' ', TIME(TBPiut.TglUpdate)) AS TglTrans
                , TBPiut.JenisInvoice as JenisInvoice 
                , TBPiutB.JMLBayar AS JmlPiut
                , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGARTBPiutDB TBPiutB
                LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
            WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'
            AND TBPiutB.TglBayar <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.2 AS JenisTrans
                , m.IdMCabang
                , M.IdTGiroCair AS IdTrans 
                , M.BuktiTGiroCair AS BuktiTrans 
                , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , -BPiutDB.JMLBayar AS JmlPiut 
                , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan 
                , -1 as Id_bcf
            FROM MGKBTGiroCairD D
                LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroCair <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.3 AS JenisTrans
                , m.IdMCabang
                , M.IdTGiroTolak AS IdTrans
                , M.BuktiTGiroTolak AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , BPiutDB.JMLBayar AS JmlPiut
                , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGKBTGiroTolakD D
                LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0) 
                AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G' 
                AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroTolak <= '${tanggal} 23:59:59'
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang 
                , MCust.IdMCust AS IdMCust 
                , 4.4 AS JenisTrans
                , m.IdMCabang
                , M.IDTGiroGanti AS IdTrans
                , M.BuktiTGiroGanti AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , -D.JMLBayar AS JmlPiut
                , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGKBTGiroGanti M
                LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0)
                AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
                AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroGanti > '${tanggal} 23:59:59' and TglTGiroGanti < '1899-12-30 00:00:00'
            UNION ALL 
            SELECT IdMCabangMCust 
                , IdMCust 
                , 5 as JenisTrans 
                , IdMCabang 
                , IdTKorPiut as IdTrans 
                , BuktiTKorPiut as BuktiTrans 
                , concat(Date(TglTKorPiut), ' ', Time(TglUpdate)) as TglTrans 
                , IdMJenisInvoice as JenisInvoice 
                , Total AS JmlPiut 
                , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTKorPiut 
            WHERE Hapus = 0 AND Void = 0
            AND Total <> 0 
            AND TglTKorPiut <= '${tanggal} 23:59:59'
            UNION ALL 
        SELECT mj.IdMCabangMCust
                , mj.IdMCust
                , 6 AS JenisTrans
                , m.IdMCabang
                , m.IdTTagihan AS IdTrans
                , m.BuktiTTagihan AS BuktiTrans
                , CONCAT(DATE(TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
                , 0 AS JenisInvoice
                , -d.jmlbayar AS JmlPiut
                , CONCAT('Bayar Tagihan No. Jual : ',mj.buktiTJual,IF(mg.KdMGiro<>'',CONCAT('(',mg.KdMGiro,')'),'')) AS Keterangan
                , -1 as Id_bcf
        FROM MGARTTagihanD d
                LEFT OUTER JOIN MGARTTagihan m ON (d.IdTTagihan=m.IdTTagihan)
                LEFT OUTER JOIN MGARTJual mj ON (d.IdTrans=mj.IdTJual)
            LEFT OUTER JOIN MGKBMGiro mg ON (d.IdMRef=mg.IdMGiro AND jenisMRef='G')
        WHERE m.Hapus =0 AND m.Void = 0
                AND d.jmlbayar <> 0
            AND TglTTagihan <= '${tanggal} 23:59:59'
        UNION ALL 
        SELECT mj.IdMCabang
                , mj.IdMCust AS IdMCust
                , 6.1 AS JenisTrans
                , m.IdMCabang
                , m.IdTTagihan AS IdTrans
                , m.BuktiTTagihan AS BuktiTrans
                , CONCAT(DATE(m.TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
                , sum(d.JMLBayar) AS JmlPiut
                , CONCAT('Titipan Giro ', m.buktiTTagihan,' (',g.KdMGiro,')' ) AS Keterangan
                , -1 as Id_bcf
        FROM MGARTTagihanD D
                LEFT OUTER JOIN mgarttagihan m ON (d.IdTTagihan=m.IdTTagihan AND d.IdMCabang=m.IdMCabang)
                LEFT OUTER JOIN mgartjual mj ON (d.idMCabang=mj.IdMCabang AND d.IdTrans=mj.IdTJual)
                LEFT OUTER JOIN mgkbmgiro g ON (d.IdMRef=g.IdMGiro AND d.IdMCabang=g.IdMCabang)
        WHERE (m.HAPUS = 0 AND m.VOID = 0) AND d.JenisMREF = 'G'
            AND TglTTagihan <= '${tanggal} 23:59:59'
        Group By Keterangan
        Union ALL
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.2 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroCair AS IdTrans
            , M.BuktiTGiroCair AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -sum(mtd.JMLBayar) AS JmlPiut
            , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroCairD D
            LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF ='G')
            LEFT OUTER JOIN mgarttagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
        WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND mtd.JenisMRef = 'G' AND mt.VOID = 0 AND mt.HAPUS = 0
            AND TglTGiroCair <= '${tanggal} 23:59:59'
        group by IdMCust
        Union ALL
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.3 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroTolak AS IdTrans
            , M.BuktiTGiroTolak AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
            , 0 AS JenisInvoice
            , sum(mtd.JMLBayar) AS JmlPiut
            , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroTolakD D
            LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTTagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
        WHERE (M.HAPUS =0 AND M.VOID = 00)
            AND M.JenisTGiroTolak = 'M' AND mtd.JenisMRef = 'G'
            AND mt.VOID = 0 AND mt.HAPUS = 0
            AND TglTGiroTolak <= '${tanggal} 23:59:59'
        group by IdMCust
        union all
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.4 AS JenisTrans
            , m.IdMCabang
            , M.IDTGiroGanti AS IdTrans
            , M.BuktiTGiroGanti AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -D.JMLBayar AS JmlPiut
            , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroGanti M
            LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTAgihanD TTagihD ON (TTagihD.IdMCabang = MG.IdMCabang AND TTagihd.IdMRef = MG.IdMGiro AND TTagihD.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTTAgihan TTagih ON (TTagih.IdMCabang = TTagihD.IdMCabang AND TTagih.IdTTagihan = TTagihD.IdTTagihan)
        WHERE (M.HAPUS = 0 AND M.VOID = 0)
            AND M.JenisTGiroGanti = 'M' AND TTagihD.JenisMRef = 'G'
            AND TTagih.VOID = 0 AND TTagih.HAPUS = 0
            AND TglTGiroGanti <= '${tanggal} 23:59:59'
        ) Tbl
            LEFT OUTER JOIN MGARMJenisInvoice JI on (Tbl.JenisInvoice = JI.IdMJenisInvoice and Tbl.IdMCabang = JI.IdMCabang)
            UNION ALL
            SELECT '${tanggal} 00:00:00' as TglTrans, IdMCabang, IdMCabang as IdMCabangMCust, IdMCust, 0 as JmlPiut, -1 as Id_bcf 
            FROM MGARMCust
            ) TransAll
            WHERE TglTrans <= '${tanggal} 23:59:59'
            GROUP BY TransAll.IdMCabang, TransAll.IdMCabangMCust, IdMCust
        ) TablePosPiut LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosPiut.IdMCabang = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGARMCust MCust ON (TablePosPiut.IdMCabangMCust = MCust.IdMCabang AND TablePosPiut.IdMCust = MCust.IdMCust)
        WHERE MCabang.Hapus = 0
            AND MCabang.KdMCabang LIKE '%%'
            AND MCabang.NmMCabang LIKE '%%'
            AND MCust.Hapus = 0
            AND MCust.KdMCust LIKE '%%'
            AND MCust.NmMCust LIKE '%%'
            AND PosPiut <> 0
        ORDER BY MCabang.KdMCabang, MCust.NmMCust
    `;
    }

    return sql;
}

exports.queryKartuPiutang = async (companyid, start, end) => { 
    var sql = ``;
    if (companyid == fun.companyWI) {
        sql = `SELECT MCabang.KdMCabang
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
        AND MCabang.KdMCabang LIKE '%%'
        AND MCabang.NmMCabang LIKE '%%'
        AND MCust.Hapus = 0
        AND MCust.Aktif = 1
        AND MCust.KdMCust LIKE '%%'
        AND MCust.NmMCust LIKE '%%'
        ORDER BY MCabang.KdMCabang, MCabang.NmMCabang, TableKartuPiut.IdMCabangMCust
            , MCust.KdMCust, MCust.NmMCust, Urut, TglTrans, JenisTrans, IdTrans
    `;  
    } else {
        sql = `SELECT MCabang.KdMCabang
        , MCabang.NmMCabang
        , Coalesce(MCust.KdMCust, '') as KdMCust
        , coalesce(MCust.NmMCust, '') as NmMCust
        , Bcf.no_bcf
        , CAST_INT(TableKartuPiut.IdMCabangMCust) As IdMCabangMCust
        , TableKartuPiut.IdMCust
        , TableKartuPiut.IdMCabangTrans
        , TableKartuPiut.IdTrans
        , TableKartuPiut.JenisTrans
        , Urut
        , BuktiTrans
        , cast(TglTrans as datetime) as TglTrans
        , TableKartuPiut.Keterangan, Saldo, JmlPiut
        , IF(Urut = 0, 0, IF(Coalesce(JmlPiut,0) > 0, Coalesce(JmlPiut,0), 0)) As Debit
        , IF(Urut = 0, 0, IF(Coalesce(JmlPiut,0) >= 0, 0, Coalesce(JmlPiut,0))) As Kredit
    FROM (
    SELECT IdMCabangTrans, IdMCabangMCust, IdMCust, 0 As Urut, 0 as JenisTrans, 0 as IdTrans, '-' As BuktiTrans, cast('${start} 00:00:00' as Date) As TglTrans, 0 As JmlPiut, sum(JmlPiut) As Saldo, 'Saldo Sebelumnya' As Keterangan
    , Id_bcf
    FROM (
        SELECT IdMCabang as IdMCabangTrans, IdMCabang as IdMCabangMCust, IdMCust, 0 As JmlPiut, -1 as id_bcf FROM MGARMCust
        UNION ALL
        SELECT LKartuPiut.IdMCabang as IdMCabangTrans, LKartuPiut.IdMCabangMCust, LKartuPiut.IdMCust, Sum(LKartuPiut.JmlPiut) as JmlPiut
        ,LKartuPiut.id_bcf
        FROM (
        SELECT IdMCabang as IdMCabangMCust 
            , IdMCust 
            , 0 as JenisTrans 
            , IdMCabang 
            , IdTSAPiut as IdTrans 
            , BuktiTSAPiut as BuktiTrans 
            , concat(Date(TglTSAPiut), ' ', Time(TglTSAPiut)) as TglTrans 
            , 0 as JenisInvoice 
            , JmlPiut 
            , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan 
            , -1 as Id_bcf
        FROM MGARTSAPiut 
        WHERE JmlPiut <> 0 
        AND TglTSAPiut < '${start} 00:00:00'
        UNION ALL 
        SELECT IdMCabangMCust 
            , IdMCust 
            , 1 as JenisTrans 
            , IdMCabang 
            , IdTJualPOS as IdTrans 
            , BuktiTJualPOS as BuktiTrans 
            , concat(Date(TglTJualPOS), ' ', Time(TglUpdate)) as TglTrans 
            , 0 as JenisInvoice 
            , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut 
            , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan 
            , -1 as Id_bcf
        FROM MGARTJualPOS 
        WHERE Hapus = 0 AND Void = 0 
        AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0 
        AND TglTJualPOS < '${start} 00:00:00'
        UNION ALL 
        SELECT Jual.IdMCabangMCust 
            , Jual.IdMCust 
            , 2 as JenisTrans 
            , Jual.IdMCabang 
            , Jual.IdTJual as IdTrans 
            , Jual.BuktiTJual as BuktiTrans 
            , concat(Date(Jual.TglTJual), ' ', Time(Jual.TglUpdate)) as TglTrans 
            , Jual.JenisTJual as JenisInvoice 
            , (Jual.JmlBayarKredit) AS JmlPiut 
            , concat('Penjualan ', Jual.BuktiTJual) as Keterangan 
            , Jual.Id_bcf
        FROM MGARTJual Jual
        WHERE Jual.Hapus = 0 AND Jual.Void = 0 
        AND (Jual.JmlBayarKredit) <> 0 
        AND Coalesce(Jual.IdTRJual, 0) = 0
        AND Jual.TglTJual < '${start} 00:00:00'
        UNION ALL 
        SELECT rj.IdMCabangMCust 
            , rj.IdMCust 
            , 3 as JenisTrans 
            , rj.IdMCabang 
            , rj.IdTRJual as IdTrans 
            , rj.BuktiTRJual as BuktiTrans 
            , concat(Date(rj.TglTRJual), ' ', Time(rj.TglUpdate)) as TglTrans 
            , rj.JenisInvoice as JenisInvoice 
            , - rj.JmlBayarKredit AS JmlPiut 
            , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan 
            , -1 as Id_bcf
        FROM MGARTRJual rj
            LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
        WHERE rj.Hapus = 0 AND rj.Void = 0 
        AND rj.JmlBayarKredit <> 0 
        AND rj.JenisRJual = 0
        AND jual.IdTJual IS NULL
        AND TglTRJual < '${start} 00:00:00'
        UNION ALL 
        SELECT TJualLain.IdMCabangCust as IdMCabangMCust 
            , TJualLain.IdMCust as IdMCust 
            , 2.1 as JenisTrans 
            , TJualLain.IdMCabang 
            , TJualLain.IdTJualLain as IdTrans 
            , TJualLain.BuktiTJualLain as BuktiTrans 
            , concat(Date(TJualLain.TglTJualLain), ' ', Time(TJualLain.TglUpdate)) as TglTrans 
            , 0 as JenisInvoice 
            , TJualLain.Netto AS JmlPiut 
            , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
                IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
                , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
            , -1 as Id_bcf
        FROM MGARTJualLain TJualLain
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
        WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0 
        AND TJualLain.Netto <> 0 
        AND TglTJualLain < '${start} 00:00:00'
        UNION ALL 
        SELECT TAngkutan.IdMCabangMCust
            , TAngkutan.IdMCust
            , 2.2 AS JenisTrans
            , TAngkutan.IdMCabang
            , TAngkutan.IdTTAngkutan AS IdTrans
            , TAngkutan.BuktiTTAngkutan AS BuktiTrans
            , CONCAT(DATE(TAngkutan.TglTTAngkutan), ' ', TIME(TAngkutan.TglUpdate)) AS TglTrans
            , TAngkutan.JenisTTAngkutan AS JenisInvoice
            , (TAngkutan.JmlBayarKredit) AS JmlPiut
            , CONCAT('Titip Angkutan ', TAngkutan.BuktiTTAngkutan) AS Keterangan
            , -1 as Id_bcf
        FROM MGARTTAngkutan TAngkutan
        WHERE Hapus = 0 AND Void = 0
        AND (TAngkutan.JmlBayarKredit) <> 0
        AND TglTTAngkutan < '${start} 00:00:00'
        UNION ALL 
        SELECT TJualManAset.IdMCabangCust as IdMCabangMCust
            , TJualManAset.IdMCust
            , 2.3 AS JenisTrans
            , TJualManAset.IdMCabang
            , TJualManAset.IdTJualAset AS IdTrans
            , TJualManAset.BuktiTJualAset AS BuktiTrans
            , CONCAT(DATE(TJualManAset.TglTJualAset), ' ', TIME(TJualManAset.TglUpdate)) AS TglTrans
            , TJualManAset.JenisTJualAset AS JenisInvoice
            , (TJualManAset.JmlBayarKredit) AS JmlPiut
            , CONCAT('Penjualan Aset ', TJualManAset.BuktiTJualAset) AS Keterangan
            , -1 as Id_bcf
        FROM MGARTJualAset TJualManAset
        WHERE Hapus = 0 AND Void = 0
        AND (TJualManAset.JmlBayarKredit) <> 0
        AND TglTJualAset < '${start} 00:00:00'
        UNION ALL 
        SELECT Tbl.IdMCabangMCust 
            , Tbl.IdMCust 
            , Tbl.JenisTrans 
            , Tbl.IdMCabang 
            , Tbl.IdTrans 
            , Tbl.BuktiTrans 
            , Tbl.TglTrans 
            , Tbl.JenisInvoice 
            , Tbl.JmlPiut + IF(SUM(UMJualD.JmlBayar) IS NULL, 0, SUM(UMJualD.JmlBayar)) AS JmlPiut 
            , Tbl.Keterangan 
            , Tbl.Id_bcf 
        FROM ( 
        SELECT m.IdMCabangMCust 
            , m.IdMCust 
            , 4 as JenisTrans 
            , m.IdMCabang 
            , m.IdTBPiut as IdTrans 
            , m.BuktiTBPiut as BuktiTrans 
            , concat(Date(d.TglBayar), ' ', Time(m.TglUpdate)) as TglTrans 
            , m.JenisInvoice as JenisInvoice 
            , - sum(d.JmlBayar) AS JmlPiut 
            , concat('Pembayaran Piutang ', BuktiTBPiut, ' ' ,if(d.jenisMref = 'K',Kas.KdMKas, if(d.JenisMRef ='B',Rek.KdMRek, if(d.JenisMref ='G' ,Giro.KdMGiro,IF(d.JenisMRef = 'P',Prk.KdMPrk,''))))) as Keterangan  
            , d.IdMCabangMRef, d.IdMref, d.TglBayar
            , m.Id_bcf
        FROM MGARTBPiutDB d 
            LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
            LEFT OUTER JOIN MGKBMKas Kas ON(Kas.IdMCabang = d.IdMCabangMref AND Kas.IdMKas = d.IdMref and d.JenisMRef ='K')
            LEFT OUTER JOIN MGKBMRek Rek ON(Rek.IdMCabang = d.IdMCabangMref AND Rek.IdMRek = d.IdMref and d.JenisMRef ='B')
            LEFT OUTER JOIN MGKBMGiro Giro ON(Giro.IdMCabang = d.IdMCabangMref AND Giro.IdMGiro = d.IdMref and d.JenisMRef ='G')
            LEFT OUTER JOIN MGGLMPrk Prk ON( Prk.IdMPrk = d.IdMref and d.JenisMRef ='P' and Prk.Periode = 0)
        WHERE m.Hapus = 0 AND m.Void = 0
        AND m.Total <> 0 
        AND d.TglBayar < '${start} 00:00:00'
        GROUP BY m.IdMCabang, m.IdTBPiut
        , d.IdMCabangMRef, d.IdMRef, d.TglBayar
        ) Tbl LEFT OUTER JOIN MGARTUMJual UMJual ON(Tbl.IdMCabang = UMJual.IdMCabangTBPiut AND Tbl.IdTrans = UMJual.IdTBPiut AND UMJual.Hapus = 0 AND UMJual.Void = 0)
            LEFT OUTER JOIN MGARTUMJualD UMJualD ON(UMJual.IdMCabang = UMJualD.IdMCabang AND UMJual.IdTUMJual = UMJualD.IdTUMJual AND Tbl.IdMCabangMRef = UMJualD.IdMCabangMRef AND Tbl.IdMref = UMJualD.IdMRef AND Tbl.TglBayar = UMJualD.TglBayar)
    GROUP BY Tbl.IdMCabangMCust
            , Tbl.IdMCust
            , Tbl.JenisTrans
            , Tbl.IdMCabang
            , Tbl.IdTrans
            , Tbl.BuktiTrans
            , Tbl.TglTrans
            , Tbl.JenisInvoice
            , Tbl.JmlPiut
            , Tbl.Keterangan
            , Tbl.Id_bcf
        UNION ALL 
        SELECT m.IdMCabangMCust 
            , m.IdMCust 
            , 4 as JenisTrans 
            , m.IdMCabang 
            , m.IdTBPiut as IdTrans 
            , m.BuktiTBPiut as BuktiTrans 
            , concat(Date(m.TglTBPiut), ' ', Time(m.TglUpdate)) as TglTrans 
            , m.JenisInvoice as JenisInvoice 
            , - m.JmlUM AS JmlPiut 
            , concat('Pembayaran Piutang dgn Uang Muka ', BuktiTBPiut) as Keterangan  
            , m.Id_bcf
        FROM MGARTBPiut m
        WHERE m.Hapus = 0 AND m.Void = 0
        AND m.JmlUM <> 0 
        AND TglTBPiut < '${start} 00:00:00'
        UNION ALL 
        SELECT MCust.IdMCabang
            , MCust.IdMCust AS IdMCust
            , 4.1 AS JenisTrans
            , TBPiut.IdMCabang
            , TBPiut.IdTBPiut AS IdTrans
            , TBPiut.BuktiTBPiut AS BuktiTrans
            , CONCAT(DATE(TBPiutB.TglBayar), ' ', TIME(TBPiut.TglUpdate)) AS TglTrans
            , TBPiut.JenisInvoice as JenisInvoice 
            , TBPiutB.JMLBayar AS JmlPiut
            , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
        FROM MGARTBPiutDB TBPiutB
            LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
        WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'
        AND TBPiutB.TglBayar < '${start} 00:00:00'
        UNION ALL 
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 4.2 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroCair AS IdTrans 
            , M.BuktiTGiroCair AS BuktiTrans 
            , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
            , BPiut.JenisInvoice as JenisInvoice 
            , -BPiutDB.JMLBayar AS JmlPiut 
            , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan 
            , -1 as Id_bcf
        FROM MGKBTGiroCairD D
            LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
        WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
        AND TglTGiroCair < '${start} 00:00:00'
        UNION ALL 
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 4.3 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroTolak AS IdTrans
            , M.BuktiTGiroTolak AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
            , BPiut.JenisInvoice as JenisInvoice 
            , BPiutDB.JMLBayar AS JmlPiut
            , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
        FROM MGKBTGiroTolakD D
            LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
        WHERE (M.HAPUS = 0 AND M.VOID = 0) 
            AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G' 
            AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
        AND TglTGiroTolak < '${start} 00:00:00'
        UNION ALL 
        SELECT MCust.IdMCabang AS IdMCabang 
            , MCust.IdMCust AS IdMCust 
            , 4.4 AS JenisTrans
            , m.IdMCabang
            , M.IDTGiroGanti AS IdTrans
            , M.BuktiTGiroGanti AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
            , BPiut.JenisInvoice as JenisInvoice 
            , -D.JMLBayar AS JmlPiut
            , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
        FROM MGKBTGiroGanti M
            LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
        WHERE (M.HAPUS = 0 AND M.VOID = 0)
            AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
            AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
        AND TglTGiroGanti >= '${start} 00:00:00' and TglTGiroGanti < '1899-12-30 00:00:00'
        UNION ALL 
        SELECT IdMCabangMCust 
            , IdMCust 
            , 5 as JenisTrans 
            , IdMCabang 
            , IdTKorPiut as IdTrans 
            , BuktiTKorPiut as BuktiTrans 
            , concat(Date(TglTKorPiut), ' ', Time(TglUpdate)) as TglTrans 
            , IdMJenisInvoice as JenisInvoice 
            , Total AS JmlPiut 
            , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan 
            , -1 as Id_bcf
        FROM MGARTKorPiut 
        WHERE Hapus = 0 AND Void = 0
        AND Total <> 0 
        AND TglTKorPiut < '${start} 00:00:00'
        UNION ALL 
    SELECT mj.IdMCabangMCust
            , mj.IdMCust
            , 6 AS JenisTrans
            , m.IdMCabang
            , m.IdTTagihan AS IdTrans
            , m.BuktiTTagihan AS BuktiTrans
            , CONCAT(DATE(TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -d.jmlbayar AS JmlPiut
            , CONCAT('Bayar Tagihan No. Jual : ',mj.buktiTJual,IF(mg.KdMGiro<>'',CONCAT('(',mg.KdMGiro,')'),'')) AS Keterangan
            , -1 as Id_bcf
    FROM MGARTTagihanD d
            LEFT OUTER JOIN MGARTTagihan m ON (d.IdTTagihan=m.IdTTagihan)
            LEFT OUTER JOIN MGARTJual mj ON (d.IdTrans=mj.IdTJual)
        LEFT OUTER JOIN MGKBMGiro mg ON (d.IdMRef=mg.IdMGiro AND jenisMRef='G')
    WHERE m.Hapus =0 AND m.Void = 0
            AND d.jmlbayar <> 0
        AND TglTTagihan < '${start} 00:00:00'
    UNION ALL 
    SELECT mj.IdMCabang
            , mj.IdMCust AS IdMCust
            , 6.1 AS JenisTrans
            , m.IdMCabang
            , m.IdTTagihan AS IdTrans
            , m.BuktiTTagihan AS BuktiTrans
            , CONCAT(DATE(m.TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
        , 0 AS JenisInvoice
            , sum(d.JMLBayar) AS JmlPiut
            , CONCAT('Titipan Giro ', m.buktiTTagihan,' (',g.KdMGiro,')' ) AS Keterangan
            , -1 as Id_bcf
    FROM MGARTTagihanD D
            LEFT OUTER JOIN mgarttagihan m ON (d.IdTTagihan=m.IdTTagihan AND d.IdMCabang=m.IdMCabang)
            LEFT OUTER JOIN mgartjual mj ON (d.idMCabang=mj.IdMCabang AND d.IdTrans=mj.IdTJual)
            LEFT OUTER JOIN mgkbmgiro g ON (d.IdMRef=g.IdMGiro AND d.IdMCabang=g.IdMCabang)
    WHERE (m.HAPUS = 0 AND m.VOID = 0) AND d.JenisMREF = 'G'
        AND TglTTagihan < '${start} 00:00:00'
    Group By Keterangan
    Union ALL
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 6.2 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroCair AS IdTrans
        , M.BuktiTGiroCair AS BuktiTrans
        , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
        , 0 AS JenisInvoice
        , -sum(mtd.JMLBayar) AS JmlPiut
        , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan
            , -1 as Id_bcf
    FROM MGKBTGiroCairD D
        LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF ='G')
        LEFT OUTER JOIN mgarttagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
    WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND mtd.JenisMRef = 'G' AND mt.VOID = 0 AND mt.HAPUS = 0
        AND TglTGiroCair < '${start} 00:00:00'
    group by IdMCust
    Union ALL
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 6.3 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroTolak AS IdTrans
        , M.BuktiTGiroTolak AS BuktiTrans
        , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
        , 0 AS JenisInvoice
        , sum(mtd.JMLBayar) AS JmlPiut
        , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
    FROM MGKBTGiroTolakD D
        LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTTagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
    WHERE (M.HAPUS =0 AND M.VOID = 00)
        AND M.JenisTGiroTolak = 'M' AND mtd.JenisMRef = 'G'
        AND mt.VOID = 0 AND mt.HAPUS = 0
        AND TglTGiroTolak < '${start} 00:00:00'
    
    group by IdMCust
    union all
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 6.4 AS JenisTrans
        , m.IdMCabang
        , M.IDTGiroGanti AS IdTrans
        , M.BuktiTGiroGanti AS BuktiTrans
        , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
        , 0 AS JenisInvoice
        , -D.JMLBayar AS JmlPiut
        , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
    FROM MGKBTGiroGanti M
        LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTTAgihanD TTagihD ON (TTagihD.IdMCabang = MG.IdMCabang AND TTagihd.IdMRef = MG.IdMGiro AND TTagihD.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTTAgihan TTagih ON (TTagih.IdMCabang = TTagihD.IdMCabang AND TTagih.IdTTagihan = TTagihD.IdTTagihan)
    WHERE (M.HAPUS = 0 AND M.VOID = 0)
        AND M.JenisTGiroGanti = 'M' AND TTagihD.JenisMRef = 'G'
        AND TTagih.VOID = 0 AND TTagih.HAPUS = 0
        AND TglTGiroGanti < '${start} 00:00:00'
        ) LKartuPiut
        left outer join mgarmjenisinvoice m on (lkartupiut.jenisinvoice = m.idmjenisinvoice)
        WHERE TglTrans < '${start} 00:00:00'
    GROUP BY LKartuPiut.IdMCabang, LKartuPiut.IdMCabangMCust, LKartuPiut.IdMCust
    ) TableSaldoAwal
    GROUP BY IdMCabangTrans, IdMCabangMCust, IdMCust
    UNION ALL
    SELECT LKartuPiut.IdMCabang as IdMCabangTrans, LKartuPiut.IdMCabangMCust, LKartuPiut.IdMCust, 1 as Urut, LKartuPiut.JenisTrans, LKartuPiut.IdTrans, LKartuPiut.BuktiTrans, LKartuPiut.TglTrans, LKartuPiut.JmlPiut, 0, LKartuPiut.Keterangan
        ,LKartuPiut.id_bcf
    FROM (
        SELECT IdMCabang as IdMCabangMCust 
            , IdMCust 
            , 0 as JenisTrans 
            , IdMCabang 
            , IdTSAPiut as IdTrans 
            , BuktiTSAPiut as BuktiTrans 
            , concat(Date(TglTSAPiut), ' ', Time(TglTSAPiut)) as TglTrans 
            , 0 as JenisInvoice 
            , JmlPiut 
            , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan 
            , -1 as Id_bcf
        FROM MGARTSAPiut 
        WHERE JmlPiut <> 0 
        AND TglTSAPiut >= '${start} 00:00:00' and TglTSAPiut <= '${end} 23:59:59'
        UNION ALL 
        SELECT IdMCabangMCust 
            , IdMCust 
            , 1 as JenisTrans 
            , IdMCabang 
            , IdTJualPOS as IdTrans 
            , BuktiTJualPOS as BuktiTrans 
            , concat(Date(TglTJualPOS), ' ', Time(TglUpdate)) as TglTrans 
            , 0 as JenisInvoice 
            , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut 
            , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan 
            , -1 as Id_bcf
        FROM MGARTJualPOS 
        WHERE Hapus = 0 AND Void = 0 
        AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0 
        AND TglTJualPOS >= '${start} 00:00:00' and TglTJualPOS <= '${end} 23:59:59'
        UNION ALL 
        SELECT Jual.IdMCabangMCust 
            , Jual.IdMCust 
            , 2 as JenisTrans 
            , Jual.IdMCabang 
            , Jual.IdTJual as IdTrans 
            , Jual.BuktiTJual as BuktiTrans 
            , concat(Date(Jual.TglTJual), ' ', Time(Jual.TglUpdate)) as TglTrans 
            , Jual.JenisTJual as JenisInvoice 
            , (Jual.JmlBayarKredit) AS JmlPiut 
            , concat('Penjualan ', Jual.BuktiTJual) as Keterangan 
            , Jual.Id_bcf
        FROM MGARTJual Jual
        WHERE Jual.Hapus = 0 AND Jual.Void = 0 
        AND (Jual.JmlBayarKredit) <> 0 
        AND Coalesce(Jual.IdTRJual, 0) = 0
        AND Jual.TglTJual >= '${start} 00:00:00' and Jual.TglTJual <= '${end} 23:59:59'
        UNION ALL 
        SELECT rj.IdMCabangMCust 
            , rj.IdMCust 
            , 3 as JenisTrans 
            , rj.IdMCabang 
            , rj.IdTRJual as IdTrans 
            , rj.BuktiTRJual as BuktiTrans 
            , concat(Date(rj.TglTRJual), ' ', Time(rj.TglUpdate)) as TglTrans 
            , rj.JenisInvoice as JenisInvoice 
            , - rj.JmlBayarKredit AS JmlPiut 
            , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan 
            , -1 as Id_bcf
        FROM MGARTRJual rj
            LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
        WHERE rj.Hapus = 0 AND rj.Void = 0 
        AND rj.JmlBayarKredit <> 0 
        AND rj.JenisRJual = 0
        AND jual.IdTJual IS NULL
        AND TglTRJual >= '${start} 00:00:00' and TglTRJual <= '${end} 23:59:59'
        UNION ALL 
        SELECT TJualLain.IdMCabangCust as IdMCabangMCust 
            , TJualLain.IdMCust as IdMCust 
            , 2.1 as JenisTrans 
            , TJualLain.IdMCabang 
            , TJualLain.IdTJualLain as IdTrans 
            , TJualLain.BuktiTJualLain as BuktiTrans 
            , concat(Date(TJualLain.TglTJualLain), ' ', Time(TJualLain.TglUpdate)) as TglTrans 
            , 0 as JenisInvoice 
            , TJualLain.Netto AS JmlPiut 
            , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
                IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
                , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
            , -1 as Id_bcf
        FROM MGARTJualLain TJualLain
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
        WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0 
        AND TJualLain.Netto <> 0 
        AND TglTJualLain >= '${start} 00:00:00' and TglTJualLain <= '${end} 23:59:59'
        UNION ALL 
        SELECT TAngkutan.IdMCabangMCust
            , TAngkutan.IdMCust
            , 2.2 AS JenisTrans
            , TAngkutan.IdMCabang
            , TAngkutan.IdTTAngkutan AS IdTrans
            , TAngkutan.BuktiTTAngkutan AS BuktiTrans
            , CONCAT(DATE(TAngkutan.TglTTAngkutan), ' ', TIME(TAngkutan.TglUpdate)) AS TglTrans
            , TAngkutan.JenisTTAngkutan AS JenisInvoice
            , (TAngkutan.JmlBayarKredit) AS JmlPiut
            , CONCAT('Titip Angkutan ', TAngkutan.BuktiTTAngkutan) AS Keterangan
            , -1 as Id_bcf
        FROM MGARTTAngkutan TAngkutan
        WHERE Hapus = 0 AND Void = 0
        AND (TAngkutan.JmlBayarKredit) <> 0
        AND TglTTAngkutan >= '${start} 00:00:00' and TglTTAngkutan <= '${end} 23:59:59'
        UNION ALL 
        SELECT TJualManAset.IdMCabangCust as IdMCabangMCust
            , TJualManAset.IdMCust
            , 2.3 AS JenisTrans
            , TJualManAset.IdMCabang
            , TJualManAset.IdTJualAset AS IdTrans
            , TJualManAset.BuktiTJualAset AS BuktiTrans
            , CONCAT(DATE(TJualManAset.TglTJualAset), ' ', TIME(TJualManAset.TglUpdate)) AS TglTrans
            , TJualManAset.JenisTJualAset AS JenisInvoice
            , (TJualManAset.JmlBayarKredit) AS JmlPiut
            , CONCAT('Penjualan Aset ', TJualManAset.BuktiTJualAset) AS Keterangan
            , -1 as Id_bcf
        FROM MGARTJualAset TJualManAset
        WHERE Hapus = 0 AND Void = 0
        AND (TJualManAset.JmlBayarKredit) <> 0
        AND TglTJualAset >= '${start} 00:00:00' and TglTJualAset <= '${end} 23:59:59'
        UNION ALL 
        SELECT Tbl.IdMCabangMCust 
            , Tbl.IdMCust 
            , Tbl.JenisTrans 
            , Tbl.IdMCabang 
            , Tbl.IdTrans 
            , Tbl.BuktiTrans 
            , Tbl.TglTrans 
            , Tbl.JenisInvoice 
            , Tbl.JmlPiut + IF(SUM(UMJualD.JmlBayar) IS NULL, 0, SUM(UMJualD.JmlBayar)) AS JmlPiut 
            , Tbl.Keterangan 
            , Tbl.Id_bcf 
        FROM ( 
        SELECT m.IdMCabangMCust 
            , m.IdMCust 
            , 4 as JenisTrans 
            , m.IdMCabang 
            , m.IdTBPiut as IdTrans 
            , m.BuktiTBPiut as BuktiTrans 
            , concat(Date(d.TglBayar), ' ', Time(m.TglUpdate)) as TglTrans 
            , m.JenisInvoice as JenisInvoice 
            , - sum(d.JmlBayar) AS JmlPiut 
            , concat('Pembayaran Piutang ', BuktiTBPiut, ' ' ,if(d.jenisMref = 'K',Kas.KdMKas, if(d.JenisMRef ='B',Rek.KdMRek, if(d.JenisMref ='G' ,Giro.KdMGiro,IF(d.JenisMRef = 'P',Prk.KdMPrk,''))))) as Keterangan  
            , d.IdMCabangMRef, d.IdMref, d.TglBayar
            , m.Id_bcf
        FROM MGARTBPiutDB d 
            LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
            LEFT OUTER JOIN MGKBMKas Kas ON(Kas.IdMCabang = d.IdMCabangMref AND Kas.IdMKas = d.IdMref and d.JenisMRef ='K')
            LEFT OUTER JOIN MGKBMRek Rek ON(Rek.IdMCabang = d.IdMCabangMref AND Rek.IdMRek = d.IdMref and d.JenisMRef ='B')
            LEFT OUTER JOIN MGKBMGiro Giro ON(Giro.IdMCabang = d.IdMCabangMref AND Giro.IdMGiro = d.IdMref and d.JenisMRef ='G')
            LEFT OUTER JOIN MGGLMPrk Prk ON( Prk.IdMPrk = d.IdMref and d.JenisMRef ='P' and Prk.Periode = 0)
        WHERE m.Hapus = 0 AND m.Void = 0
        AND m.Total <> 0 
        AND d.TglBayar >= '${start} 00:00:00' and d.TglBayar <= '${end} 23:59:59'
        GROUP BY m.IdMCabang, m.IdTBPiut
        , d.IdMCabangMRef, d.IdMRef, d.TglBayar
        ) Tbl LEFT OUTER JOIN MGARTUMJual UMJual ON(Tbl.IdMCabang = UMJual.IdMCabangTBPiut AND Tbl.IdTrans = UMJual.IdTBPiut AND UMJual.Hapus = 0 AND UMJual.Void = 0)
            LEFT OUTER JOIN MGARTUMJualD UMJualD ON(UMJual.IdMCabang = UMJualD.IdMCabang AND UMJual.IdTUMJual = UMJualD.IdTUMJual AND Tbl.IdMCabangMRef = UMJualD.IdMCabangMRef AND Tbl.IdMref = UMJualD.IdMRef AND Tbl.TglBayar = UMJualD.TglBayar)
    GROUP BY Tbl.IdMCabangMCust
            , Tbl.IdMCust
            , Tbl.JenisTrans
            , Tbl.IdMCabang
            , Tbl.IdTrans
            , Tbl.BuktiTrans
            , Tbl.TglTrans
            , Tbl.JenisInvoice
            , Tbl.JmlPiut
            , Tbl.Keterangan
            , Tbl.Id_bcf
        UNION ALL 
        SELECT m.IdMCabangMCust 
            , m.IdMCust 
            , 4 as JenisTrans 
            , m.IdMCabang 
            , m.IdTBPiut as IdTrans 
            , m.BuktiTBPiut as BuktiTrans 
            , concat(Date(m.TglTBPiut), ' ', Time(m.TglUpdate)) as TglTrans 
            , m.JenisInvoice as JenisInvoice 
            , - m.JmlUM AS JmlPiut 
            , concat('Pembayaran Piutang dgn Uang Muka ', BuktiTBPiut) as Keterangan  
            , m.Id_bcf
        FROM MGARTBPiut m
        WHERE m.Hapus = 0 AND m.Void = 0
        AND m.JmlUM <> 0 
        AND TglTBPiut >= '${start} 00:00:00' and TglTBPiut <= '${end} 23:59:59'
        UNION ALL 
        SELECT MCust.IdMCabang
            , MCust.IdMCust AS IdMCust
            , 4.1 AS JenisTrans
            , TBPiut.IdMCabang
            , TBPiut.IdTBPiut AS IdTrans
            , TBPiut.BuktiTBPiut AS BuktiTrans
            , CONCAT(DATE(TBPiutB.TglBayar), ' ', TIME(TBPiut.TglUpdate)) AS TglTrans
            , TBPiut.JenisInvoice as JenisInvoice 
            , TBPiutB.JMLBayar AS JmlPiut
            , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
        FROM MGARTBPiutDB TBPiutB
            LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
        WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'
        AND TBPiutB.TglBayar >= '${start} 00:00:00' and TBPiutB.TglBayar <= '${end} 23:59:59'
        UNION ALL 
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 4.2 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroCair AS IdTrans 
            , M.BuktiTGiroCair AS BuktiTrans 
            , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
            , BPiut.JenisInvoice as JenisInvoice 
            , -BPiutDB.JMLBayar AS JmlPiut 
            , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan 
            , -1 as Id_bcf
        FROM MGKBTGiroCairD D
            LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
        WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
        AND TglTGiroCair >= '${start} 00:00:00' and TglTGiroCair <= '${end} 23:59:59'
    
        UNION ALL 
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 4.3 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroTolak AS IdTrans
            , M.BuktiTGiroTolak AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
            , BPiut.JenisInvoice as JenisInvoice 
            , BPiutDB.JMLBayar AS JmlPiut
            , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
        FROM MGKBTGiroTolakD D
            LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
        WHERE (M.HAPUS = 0 AND M.VOID = 0) 
            AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G' 
            AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
        AND TglTGiroTolak >= '${start} 00:00:00' and TglTGiroTolak <= '${end} 23:59:59'
        UNION ALL 
        SELECT MCust.IdMCabang AS IdMCabang 
            , MCust.IdMCust AS IdMCust 
            , 4.4 AS JenisTrans
            , m.IdMCabang
            , M.IDTGiroGanti AS IdTrans
            , M.BuktiTGiroGanti AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
            , BPiut.JenisInvoice as JenisInvoice 
            , -D.JMLBayar AS JmlPiut
            , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
        FROM MGKBTGiroGanti M
            LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
        WHERE (M.HAPUS = 0 AND M.VOID = 0)
            AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
            AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
        AND TglTGiroGanti >= '${start} 00:00:00' and TglTGiroGanti <= '${end} 23:59:59'
    
        UNION ALL 
        SELECT IdMCabangMCust 
            , IdMCust 
            , 5 as JenisTrans 
            , IdMCabang 
            , IdTKorPiut as IdTrans 
            , BuktiTKorPiut as BuktiTrans 
            , concat(Date(TglTKorPiut), ' ', Time(TglUpdate)) as TglTrans 
            , IdMJenisInvoice as JenisInvoice 
            , Total AS JmlPiut 
            , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan 
            , -1 as Id_bcf
        FROM MGARTKorPiut 
        WHERE Hapus = 0 AND Void = 0
        AND Total <> 0 
        AND TglTKorPiut >= '${start} 00:00:00' and TglTKorPiut <= '${end} 23:59:59'
        UNION ALL 
    SELECT mj.IdMCabangMCust
            , mj.IdMCust
            , 6 AS JenisTrans
            , m.IdMCabang
            , m.IdTTagihan AS IdTrans
            , m.BuktiTTagihan AS BuktiTrans
            , CONCAT(DATE(TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -d.jmlbayar AS JmlPiut
            , CONCAT('Bayar Tagihan No. Jual : ',mj.buktiTJual,IF(mg.KdMGiro<>'',CONCAT('(',mg.KdMGiro,')'),'')) AS Keterangan
            , -1 as Id_bcf
    FROM MGARTTagihanD d
            LEFT OUTER JOIN MGARTTagihan m ON (d.IdTTagihan=m.IdTTagihan)
            LEFT OUTER JOIN MGARTJual mj ON (d.IdTrans=mj.IdTJual)
        LEFT OUTER JOIN MGKBMGiro mg ON (d.IdMRef=mg.IdMGiro AND jenisMRef='G')
    WHERE m.Hapus =0 AND m.Void = 0
            AND d.jmlbayar <> 0
        AND TglTTagihan >= '${start} 00:00:00' and TglTTagihan <= '${end} 23:59:59'
    UNION ALL 
    SELECT mj.IdMCabang
            , mj.IdMCust AS IdMCust
            , 6.1 AS JenisTrans
            , m.IdMCabang
            , m.IdTTagihan AS IdTrans
            , m.BuktiTTagihan AS BuktiTrans
            , CONCAT(DATE(m.TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
        , 0 AS JenisInvoice
            , sum(d.JMLBayar) AS JmlPiut
            , CONCAT('Titipan Giro ', m.buktiTTagihan,' (',g.KdMGiro,')' ) AS Keterangan
            , -1 as Id_bcf
    FROM MGARTTagihanD D
            LEFT OUTER JOIN mgarttagihan m ON (d.IdTTagihan=m.IdTTagihan AND d.IdMCabang=m.IdMCabang)
            LEFT OUTER JOIN mgartjual mj ON (d.idMCabang=mj.IdMCabang AND d.IdTrans=mj.IdTJual)
            LEFT OUTER JOIN mgkbmgiro g ON (d.IdMRef=g.IdMGiro AND d.IdMCabang=g.IdMCabang)
    WHERE (m.HAPUS = 0 AND m.VOID = 0) AND d.JenisMREF = 'G'
        AND TglTTagihan >= '${start} 00:00:00' and TglTTagihan <= '${end} 23:59:59'
    Group By Keterangan
    Union ALL
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 6.2 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroCair AS IdTrans
        , M.BuktiTGiroCair AS BuktiTrans
        , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
        , 0 AS JenisInvoice
        , -sum(mtd.JMLBayar) AS JmlPiut
        , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan
            , -1 as Id_bcf
    FROM MGKBTGiroCairD D
        LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF ='G')
        LEFT OUTER JOIN mgarttagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
    WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND mtd.JenisMRef = 'G' AND mt.VOID = 0 AND mt.HAPUS = 0
        AND TglTGiroCair >= '${start} 00:00:00' and TglTGiroCair <= '${end} 23:59:59'
    group by IdMCust
    Union ALL
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 6.3 AS JenisTrans
        , m.IdMCabang
        , M.IdTGiroTolak AS IdTrans
        , M.BuktiTGiroTolak AS BuktiTrans
        , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
        , 0 AS JenisInvoice
        , sum(mtd.JMLBayar) AS JmlPiut
        , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
    FROM MGKBTGiroTolakD D
        LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTTagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
    WHERE (M.HAPUS =0 AND M.VOID = 00)
        AND M.JenisTGiroTolak = 'M' AND mtd.JenisMRef = 'G'
        AND mt.VOID = 0 AND mt.HAPUS = 0
        AND TglTGiroTolak >= '${start} 00:00:00' and TglTGiroTolak <= '${end} 23:59:59'
    group by IdMCust
    union all
    SELECT MCust.IdMCabang AS IdMCabang
        , MCust.IdMCust AS IdMCust
        , 6.4 AS JenisTrans
        , m.IdMCabang
        , M.IDTGiroGanti AS IdTrans
        , M.BuktiTGiroGanti AS BuktiTrans
        , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
        , 0 AS JenisInvoice
        , -D.JMLBayar AS JmlPiut
        , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
            , -1 as Id_bcf
    FROM MGKBTGiroGanti M
        LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
        LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
        LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
        LEFT OUTER JOIN MGARTTAgihanD TTagihD ON (TTagihD.IdMCabang = MG.IdMCabang AND TTagihd.IdMRef = MG.IdMGiro AND TTagihD.JenisMREF = 'G')
        LEFT OUTER JOIN MGARTTAgihan TTagih ON (TTagih.IdMCabang = TTagihD.IdMCabang AND TTagih.IdTTagihan = TTagihD.IdTTagihan)
    WHERE (M.HAPUS = 0 AND M.VOID = 0)
        AND M.JenisTGiroGanti = 'M' AND TTagihD.JenisMRef = 'G'
        AND TTagih.VOID = 0 AND TTagih.HAPUS = 0
        AND TglTGiroGanti >= '${start} 00:00:00' and TglTGiroGanti <= '${end} 23:00:00'
    ) LKartuPiut 
        left outer join mgarmjenisinvoice m on (lkartupiut.jenisinvoice = m.idmjenisinvoice)
    where LKartuPiut.TglTrans >= '${start} 00:00:00' and LKartuPiut.TglTrans <= '${end} 23:59:59'
    ) TableKartuPiut LEFT OUTER JOIN MGSYMCabang MCabang ON (TableKartuPiut.IdMCabangTrans = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGARMCust MCust ON (TableKartuPiut.IdMCabangMCust = MCust.IdMCabang AND TableKartuPiut.IdMCust = MCust.IdMCust)
                        LEFT OUTER JOIN bookout_bcf bcf ON (TableKartuPiut.id_bcf = bcf.id)
    WHERE MCabang.Hapus = 0
        AND MCabang.Aktif = 1
        AND MCabang.KdMCabang LIKE '%%'
        AND MCabang.NmMCabang LIKE '%%'
        AND MCust.Hapus = 0
        AND MCust.Aktif = 1
        AND MCust.KdMCust LIKE '%%'
        AND MCust.NmMCust LIKE '%%'
    ORDER BY MCabang.KdMCabang, MCabang.NmMCabang, TableKartuPiut.IdMCabangMCust
            , MCust.KdMCust, MCust.NmMCust, Urut, TglTrans, JenisTrans, IdTrans
    `;  
    }

    return sql;
}