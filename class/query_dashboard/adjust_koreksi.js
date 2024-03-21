const fun = require("../../mgmx");
var companyWI = fun.companyWI;

// INVENTORY => PENYESUAIAN STOK
exports.queryPenyesuaianStok = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }

    // QUERY WI DAN MASSAL => SAMA
    sql = `SELECT MBrg.KdMBrg, MBrg.NmMBrg, MBrg.Reserved_int1 as tipe
            , MStn1.KdMStn as KdMStn1, MStn2.KdMStn as KdMStn2, MStn3.KdMStn as KdMStn3
            , MStn4.KdMStn as KdMStn4, MStn5.KdMStn as KdMStn5
            , pb.BuktiTPenyesuaianBrg, pb.TglTPenyesuaianBrg
            , TPenyesuaianBrgD.*, SUM(TPenyesuaianBrgD.QtyTotal) as QtyTotal
        FROM MGINTPenyesuaianBrgD TPenyesuaianBrgD
        LEFT OUTER JOIN MGINMBrg MBrg ON (TPenyesuaianBrgD.IdMBrg = MBrg.IdMBrg)
        LEFT OUTER JOIN MGINMStn MStn1 ON (MBrg.IdMStn1 = MStn1.IdMStn)
        LEFT OUTER JOIN MGINMStn MStn2 ON (MBrg.IdMStn2 = MStn2.IdMStn)
        LEFT OUTER JOIN MGINMStn MStn3 ON (MBrg.IdMStn3 = MStn3.IdMStn)
        LEFT OUTER JOIN MGINMStn MStn4 ON (MBrg.IdMStn4 = MStn4.IdMStn)
        LEFT OUTER JOIN MGINMStn MStn5 ON (MBrg.IdMStn5 = MStn5.IdMStn)
        LEFT outer join MGINTPenyesuaianBrg pb on pb.IdTPenyesuaianBrg = TPenyesuaianBrgD.IdTPenyesuaianBrg 
        where pb.TglTPenyesuaianBrg >= '${start} 00:00:00' and pb.TglTPenyesuaianBrg <= '${end} 23:59:59' and pb.Hapus = 0 and pb.Void = 0
        GROUP BY MBrg.KdMBrg, pb.TglTPenyesuaianBrg, pb.BuktiTPenyesuaianBrg`

    return sql;
}

// HUTANG => KOREKSI HUTANG
exports.queryKoreksiHutang = async (companyid, start, end) => {
    var sql = ``;
    // WI
    if (companyid == companyWI) { 
        sql = `SELECT TKorHutD.IdMCabang
            , TKorHutD.IdTKorHut
            , TKorHutD.IdTKorHutD
            , TKorHutD.JenisTrans
            , TKorHutD.IdMCabangTrans
            , MSup.NmMSup
            , TKorHut.BuktiTKorHut 
            , TKorHut.TglTKorHut  
            , IF(TKorHutD.JenisTrans = 'S', '', MCabang.KdMCabang) as KdMCabangTrans
            , TKorHutD.IdTrans, TKorHutD.IdTransD
            , IF(TKorHutD.IdTrans = 0, TKorHutD.BuktiTrans, IF(TKorHutD.JenisTrans = 'S', 'Saldo Awal', IF(TKorHutD.JenisTrans = 'T', TBeli.BuktiTBeli, IF(TKorHutD.JenisTrans = 'R', TRBeli.BuktiTRBeli, '')))) as BuktiTransAll
            , TKorHutD.BuktiTrans
            -- , SUM(IF(TKorHut.JenisTKorHut = 'C', TKorHutD.JMLKOR, IF(TKorHut.JenisTKorHut = 'D', (TKorHutD.JMLKOR* -1), TKorHutD.JMLKOR))) As JmlKor
            , SUM(TKorHutD.JMLKOR) as JmlKor
            , TKorHutD.KetKor, TKorHutD.IdMPrk
            , MPrk.KdMPrk, MPrk.NmMPrk
        FROM MGAPTKorHutD TKorHutD
        LEFT OUTER JOIN MGAPTKorHut TKorHut ON (TKorHut.IdMCabang = TKorHutD.IdMCabang AND TKorHut.IdTKorHut = TKorHutD.IdTKorHut)
        LEFT OUTER JOIN MGSYMCabang MCabang ON (TKorHutD.IdMCabangTrans = MCabang.IdMCabang)
        LEFT OUTER JOIN MGAPTBeli TBeli ON (TKorHutD.JenisTrans = 'T' AND TKorHutD.IdMCabangTrans = TBeli.IdMCabang AND TKorHutD.IdTrans = TBeli.IdTBeli)
        LEFT OUTER JOIN MGAPTRBeli TRBeli ON (TKorHutD.JenisTrans = 'R' AND TKorHutD.IdMCabangTrans = TRBeli.IdMCabang AND TKorHutD.IdTrans = TRBeli.IdTRBeli)
        LEFT OUTER JOIN MGGLMPrk MPrk ON (MPrk.IdMPrk = TKorHutD.IdMPrk AND MPrk.Periode = 0)
        LEFT OUTER JOIN MGAPMSUP MSup on (TKorHut.IdMSup = MSup.IdMSup)
        WHERE TKorHut.TglTKorHut >= '${start} 00:00:00' and TKorHut.TglTKorHut <= '${end} 23:59:59' and TKorHut.Hapus = 0 and TKorHut.Void = 0
        GROUP BY TKorHut.TglTKorHut, TKorHut.BuktiTKorHut, MSup.NmMSup`;
    } else {
        sql = `SELECT TKorHutD.IdMCabang
            , TKorHutD.IdTKorHut
            , TKorHutD.IdTKorHutD
            , TKorHutD.JenisTrans
            , TKorHutD.IdMCabangTrans
            , MSup.NmMSup
            , TKorHut.BuktiTKorHut 
            , TKorHut.TglTKorHut  
            , MCabang.KdMCabang as KdMCabangTrans
            , TKorHutD.IdTrans, TKorHutD.IdTransD
            , IF(TKorHutD.IdTrans = 0, TKorHutD.BuktiTrans, IF(TKorHutD.JenisTrans = 'S', TSAHut.BuktiTSAHut, IF(TKorHutD.JenisTrans = 'T', TBeli.BuktiTBeli, IF(TKorHutD.JenisTrans = 'R', TRBeli.BuktiTRBeli
                , IF(TKorHutD.JenisTrans = 'A', TBeliManAset.BuktiTBeliAset, ''))))) as BuktiTransAll
            , TKorHutD.BuktiTrans
            , SUM(IF(TKorHut.JenisTKorHut = 'C', TKorHutD.JMLKOR, IF(TKorHut.JenisTKorHut = 'D', (TKorHutD.JMLKOR* -1), TKorHutD.JMLKOR))) As JmlKor
            , TKorHutD.KetKor, TKorHutD.IdMPrk
            , MPrk.KdMPrk, MPrk.NmMPrk
        FROM MGAPTKorHutD TKorHutD
        LEFT OUTER JOIN MGAPTKorHut TKorHut ON (TKorHut.IdMCabang = TKorHutD.IdMCabang AND TKorHut.IdTKorHut = TKorHutD.IdTKorHut)
        LEFT OUTER JOIN MGSYMCabang MCabang ON (TKorHutD.IdMCabangTrans = MCabang.IdMCabang)
        LEFT OUTER JOIN MGAPTBeli TBeli ON (TKorHutD.JenisTrans = 'T' AND TKorHutD.IdMCabangTrans = TBeli.IdMCabang AND TKorHutD.IdTrans = TBeli.IdTBeli)
        LEFT OUTER JOIN MGAPTRBeli TRBeli ON (TKorHutD.JenisTrans = 'R' AND TKorHutD.IdMCabangTrans = TRBeli.IdMCabang AND TKorHutD.IdTrans = TRBeli.IdTRBeli)
        LEFT OUTER JOIN MGAPTSAHut TSAHut ON (TKorhutD.JenisTrans = 'S' AND TKorHutD.IdMCabangTrans = TSAHut.IdMCabang AND TKorHutD.IdTrans = TSAHut.IdTSAHut)
        LEFT OUTER JOIN MGGLMPrk MPrk ON (MPrk.IdMPrk = TKorHutD.IdMPrk AND MPrk.Periode = 0)
        LEFT OUTER JOIN MGAPTBeliAset TBeliManAset ON (TKorHutD.JenisTrans = 'A' AND TKorHutD.IdMCabangTrans = TBeliManAset.IdMCabang AND TKorHutD.IdTrans = TBeliManAset.IdTBeliAset)
        LEFT OUTER JOIN MGAPMSUP MSup on (TKorHut.IdMSup = MSup.IdMSup)
        WHERE TKorHut.TglTKorHut >= '${start} 00:00:00' and TKorHut.TglTKorHut <= '${end} 23:59:59' and TKorHut.Hapus = 0 and TKorHut.Void = 0
        GROUP BY TKorHut.TglTKorHut, TKorHut.BuktiTKorHut, MSup.NmMSup`;
    }
    return sql;
}

// PIUTANG => KOREKSI PIUTANG
exports.queryKoreksiPiutang = async (companyid, start, end) => {
    var sql = ``;
    if (companyid == companyWI) { 
        sql = `SELECT TKorPiutD.IdMCabang
            , TKorPiutD.IdTKorPiut
            , TKorPiutD.IdTKorPiutD
            , TKorPiutD.JenisTrans
            , TKorPiutD.IdMCabangTrans
            , TKorPiut.TglTKorPiut
            , TKorPiut.BuktiTKorPiut
            , MCust.NmMCust 
            , IF(TKorPiutD.JenisTrans = 'S', MCabang.KdMCabang, MCabang.KdMCabang) as KdMCabangTrans
            , TKorPiutD.IdTrans, TKorPiutD.IdTransD
            , IF(TKorPiutD.IdTrans = 0, TKorPiutD.BuktiTrans, 
                IF(TKorPiutD.JenisTrans = 'S', TSAPiut.BuktiTSAPiut, 
                IF(TKorPiutD.JenisTrans = 'T', TJualPOS.BuktiTJualPOS, 
                IF(TKorPiutD.JenisTrans = 'J', TJual.BuktiTJual, 
                IF(TKorPiutD.JenisTrans = 'R', TRJual.BuktiTRJual, 
                IF(TKorPiutD.JenisTrans = 'W', TJualLain.BuktiAsli, '')))))) AS BuktiTransAll
            , TKorPiutD.BuktiTrans
            -- , SUM(IF(TKorPiut.JenisTKorPiut = 'D', TKorPiutD.JMLKOR, IF(TKorPiut.JenisTKorPiut = 'C', (TKorPiutD.JMLKOR* -1), TKorPiutD.JMLKOR))) As JmlKor
            , SUM(TKorPiutD.JMLKOR) as JmlKor 
            , TkorPiutD.KetKor, TKorPiutD.IdMPrk
            , MPrk.KdMPrk, MPrk.NmMPrk
            , CAST_INT(0) as IdMbrg, '' as KdMbrg, '' as NmMBrg
            FROM MGARTKorPiutD TKorPiutD
            LEFT OUTER JOIN MGSYMCabang MCabang ON (TKorPiutD.IdMCabangTrans = MCabang.IdMCabang)
            LEFT OUTER JOIN MGARTJualPOS TJualPOS ON (TKorPiutD.JenisTrans = 'T' AND TKorPiutD.IdMCabangTrans = TJualPOS.IdMCabang AND TKorPiutD.IdTrans = TJualPOS.IdTJualPOS)
            LEFT OUTER JOIN MGARTJual TJual ON (TKorPiutD.JenisTrans = 'J' AND TKorPiutD.IdMCabangTrans = TJual.IdMCabang AND TKorPiutD.IdTrans = TJual.IdTJual)
            LEFT OUTER JOIN MGARTSAPiut TSAPiut ON (TKorPiutD.JenisTrans = 'S' AND TKorPiutD.IdMCabangTrans = TSAPiut.IdMCabang AND TKorPiutD.IdTrans = TSAPiut.IdTSAPiut)
            LEFT OUTER JOIN MGARTRJual TRJual ON (TKorPiutD.JenisTrans = 'R' AND TKorPiutD.IdMCabangTrans = TRJual.IdMCabang AND TKorPiutD.IdTrans = TRJual.IdTRJual)
            LEFT OUTER JOIN MGARTJualLain TJualLain ON (TKorPiutD.JenisTrans = 'W' AND TKorPiutD.IdMCabangTrans = TJualLain.IdMCabang AND TKorPiutD.IdTrans  = TJualLain.IdTJualLain)
            LEFT OUTER JOIN MGARTKorPiut TKorPiut ON (TKorPiut.IdMCabang = TKorPiutD.IdMCabang AND TKorPiut.IdTKorPiut = TKorPiutD.IdTKorPiut)
            LEFT OUTER JOIN MGGLMPrk MPrk ON (MPrk.IdMPrk = TKorPiutD.IdMPrk AND MPrk.Periode = 0)
            LEFT OUTER JOIN MGARMCUST MCust on MCust.IdMCust = TKorPiut.IdMCust
            WHERE TKorPiut.TglTKorPiut >= '${start} 00:00:00' and TKorPiut.TglTKorPiut <= '${end} 23:59:59' and TKorPiut.Hapus = 0 and TKorPiut.Void = 0
            GROUP BY TKorPiut.TglTKorPiut, TKorPiut.BuktiTKorPiut, MCust.NmMCust`;
    } else {
        sql = `SELECT TKorPiutD.IdMCabang 
            , TKorPiutD.IdTKorPiut 
            , TKorPiutD.IdTKorPiutD 
            , TKorPiutD.JenisTrans 
            , TKorPiutD.IdMCabangTrans
            , TKorPiut.TglTKorPiut
            , TKorPiut.BuktiTKorPiut
            , MCust.NmMCust 
            , IF(TKorPiutD.JenisTrans = 'S', MCabang.KdMCabang, MCabang.KdMCabang) AS KdMCabangTrans 
            , TKorPiutD.IdTrans, TKorPiutD.IdTransD 
            , IF(TKorPiutD.IdTrans = 0, TKorPiutD.BuktiTrans, IF(TKorPiutD.JenisTrans = 'S', TSAPiut.BuktiTSAPiut, IF(TKorPiutD.JenisTrans = 'T' 
                , TJualPOS.BuktiTJualPOS, IF(TKorPiutD.JenisTrans = 'J', TJual.BuktiTJual, IF(TKorPiutD.JenisTrans = 'R', TRJual.BuktiTRJual, IF(TKorPiutD.JenisTrans = 'N', TJualManAset.BuktiTJualAset, '')))))) AS BuktiTransAll 
            , TKorPiutD.BuktiTrans 
            , SUM(IF(TKorPiut.JenisTKorPiut = 'D', TKorPiutD.JMLKOR, IF(TKorPiut.JenisTKorPiut = 'C', (TKorPiutD.JMLKOR* -1), TKorPiutD.JMLKOR))) AS JmlKor 
            , TkorPiutD.KetKor, TKorPiutD.IdMPrk 
            , MPrk.KdMPrk, MPrk.NmMPrk 
            , CAST_INT(0) AS IdMbrg, '' AS KdMbrg, '' AS NmMBrg 
            FROM MGARTKorPiutD TKorPiutD 
            LEFT OUTER JOIN MGSYMCabang MCabang ON (TKorPiutD.IdMCabangTrans = MCabang.IdMCabang) 
            LEFT OUTER JOIN MGARTJualPOS TJualPOS ON (TKorPiutD.JenisTrans = 'T' AND TKorPiutD.IdMCabangTrans = TJualPOS.IdMCabang AND TKorPiutD.IdTrans = TJualPOS.IdTJualPOS) 
            LEFT OUTER JOIN MGARTJual TJual ON (TKorPiutD.JenisTrans = 'J' AND TKorPiutD.IdMCabangTrans = TJual.IdMCabang AND TKorPiutD.IdTrans = TJual.IdTJual) 
            LEFT OUTER JOIN MGARTSAPiut TSAPiut ON (TKorPiutD.JenisTrans = 'S' AND TKorPiutD.IdMCabangTrans = TSAPiut.IdMCabang AND TKorPiutD.IdTrans = TSAPiut.IdTSAPiut) 
            LEFT OUTER JOIN MGARTRJual TRJual ON (TKorPiutD.JenisTrans = 'R' AND TKorPiutD.IdMCabangTrans = TRJual.IdMCabang AND TKorPiutD.IdTrans = TRJual.IdTRJual) 
            LEFT OUTER JOIN MGARTJualAset TJualManAset ON (TKorPiutD.JenisTrans = 'N' AND TKorPiutD.IdMCabangTrans = TJualManAset.IdMCabang AND TKorPiutD.IdTrans = TJualManAset.IdTJualAset) 
            LEFT OUTER JOIN MGARTKorPiut TKorPiut ON (TKorPiut.IdMCabang = TKorPiutD.IdMCabang AND TKorPiut.IdTKorPiut = TKorPiutD.IdTKorPiut) 
            LEFT OUTER JOIN MGGLMPrk MPrk ON (MPrk.IdMPrk = TKorPiutD.IdMPrk AND MPrk.Periode = 0)
            LEFT OUTER JOIN MGARMCUST MCust on MCust.IdMCust = TKorPiut.IdMCust
            WHERE TKorPiut.TglTKorPiut >= '${start} 00:00:00' and TKorPiut.TglTKorPiut <= '${end} 23:59:59' and TKorPiut.Hapus = 0 and TKorPiut.Void = 0
            GROUP BY TKorPiut.TglTKorPiut, TKorPiut.BuktiTKorPiut, MCust.NmMCust`;
    }
    
    return sql;
}

// GENERAL LEDGER => JURNAL MEMO
exports.queryJurnalMemo = async (companyid, start, end) => { 
    var sql = ``;

    if (companyid == companyWI) { 
        sql = `SELECT j.*, SUM(j.JmlD) as JmlD, SUM(j.JmlK) as JmlK, jurnal.TglTJurnal, jurnal.BuktiTJurnal, d.KdMPrk as KdMPrkD, d.NmMPrk as NmMPrkD, k.KdMPrk as KdMPrkK, k.NmMPrk as NmMPrkK
            FROM MGGLTJurnalD j 
            LEFT OUTER JOIN MGGLMPrk d ON (j.IdMPrkD = d.IdMPrk and d.Periode = 0)
            LEFT OUTER JOIN MGGLMPrk k ON (j.IdMPrkK = k.IdMPrk and k.Periode = 0)
            LEFT OUTER JOIN MGGLTJurnal jurnal on (jurnal.IdTJurnal = j.IdTJurnal)
            WHERE jurnal.TglTJurnal >= '${start} 00:00:00' and jurnal.TglTJurnal <= '${end} 23:59:59'
            GROUP BY jurnal.TglTJurnal, jurnal.BuktiTJurnal, j.Keterangan`;
    }else{
        sql = `SELECT j.*, SUM(j.JmlD) as JmlD, SUM(j.JmlK) as JmlK
            , jurnal.TglTJurnal, jurnal.BuktiTJurnal
            , d.KdMPrk as KdMPrkD, d.NmMPrk as NmMPrkD, k.KdMPrk as KdMPrkK, k.NmMPrk as NmMPrkK
            , cast_int(IF(j.JmlD <> 0, d.IdMPrk, k.IdMPrk)) As IdMPrkDblEntry
            , IF(j.JmlD <> 0, d.KdMPrk, k.KdMPrk) As KdMPrkDblEntry
            , IF(j.JmlD <> 0, d.NmMPrk, k.NmMPrk) As NmMPrkDblEntry
            , md.KdMDivisi, md.NmMDivisi
        FROM MGGLTJurnalD j 
        LEFT OUTER JOIN MGGLMPrk d ON (j.IdMPrkD = d.IdMPrk and d.Periode = 0)
        LEFT OUTER JOIN MGGLMPrk k ON (j.IdMPrkK = k.IdMPrk and k.Periode = 0)
        LEFT OUTER JOIN MGINMDivisi md ON (md.IdMCabang = j.IdMCabangMDivisi AND md.IdMDivisi = j.IdMDivisi)
        LEFT OUTER JOIN MGGLTJurnal jurnal on (jurnal.IdTJurnal = j.IdTJurnal)
        WHERE jurnal.TglTJurnal >= '${start} 00:00:00' and jurnal.TglTJurnal <= '${end} 23:59:59'
        GROUP BY jurnal.TglTJurnal, jurnal.BuktiTJurnal, j.Keterangan`;
    }

    return sql;
}
