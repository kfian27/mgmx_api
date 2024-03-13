const fun = require("../../mgmx");
var companyWI = fun.companyWI;

exports.queryPenyesuaianStok = async (companyid, start, end) => { 
    var sql = ``;
    if (companyid == companyWI) {
        

        
    }

    sql = `SELECT MBrg.KdMBrg, MBrg.NmMBrg, MBrg.Reserved_int1 as tipe
            , MStn1.KdMStn as KdMStn1, MStn2.KdMStn as KdMStn2, MStn3.KdMStn as KdMStn3
            , MStn4.KdMStn as KdMStn4, MStn5.KdMStn as KdMStn5
            , pb.BuktiTPenyesuaianBrg, pb.TglTPenyesuaianBrg
            , TPenyesuaianBrgD.*
        FROM MGINTPenyesuaianBrgD TPenyesuaianBrgD
        LEFT OUTER JOIN MGINMBrg MBrg ON (TPenyesuaianBrgD.IdMBrg = MBrg.IdMBrg)
        LEFT OUTER JOIN MGINMStn MStn1 ON (MBrg.IdMStn1 = MStn1.IdMStn)
        LEFT OUTER JOIN MGINMStn MStn2 ON (MBrg.IdMStn2 = MStn2.IdMStn)
        LEFT OUTER JOIN MGINMStn MStn3 ON (MBrg.IdMStn3 = MStn3.IdMStn)
        LEFT OUTER JOIN MGINMStn MStn4 ON (MBrg.IdMStn4 = MStn4.IdMStn)
        LEFT OUTER JOIN MGINMStn MStn5 ON (MBrg.IdMStn5 = MStn5.IdMStn)
        LEFT outer join MGINTPenyesuaianBrg pb on pb.IdTPenyesuaianBrg = TPenyesuaianBrgD.IdTPenyesuaianBrg 
        where pb.TglTPenyesuaianBrg >= '${start} 00:00:00' and pb.TglTPenyesuaianBrg <= '${end} 23:59:59' and pb.Hapus = 0 and pb.Void = 0`

    return sql;
}

exports.queryKoreksiHutang = async (companyid, start, end) => {
    var sql = ``;
    if (companyid == companyWI) { 

    }
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
        , IF(TKorHut.JenisTKorHut = 'C', TKorHutD.JMLKOR, IF(TKorHut.JenisTKorHut = 'D', (TKorHutD.JMLKOR* -1), TKorHutD.JMLKOR)) As JmlKor
        , TKorHutD.KetKor, TKorHutD.IdMPrk
        , MPrk.KdMPrk, MPrk.NmMPrk
    FROM MGAPTKorHutD TKorHutD
        LEFT OUTER JOIN MGAPTKorHut TKorHut ON (TKorHut.IdMCabang = TKorHutD.IdMCabang AND TKorHut.IdTKorHut = TKorHutD.IdTKorHut)
        LEFT OUTER JOIN MGSYMCabang MCabang ON (TKorHutD.IdMCabangTrans = MCabang.IdMCabang)
        LEFT OUTER JOIN MGAPTBeli TBeli ON (TKorHutD.JenisTrans = 'T' AND TKorHutD.IdMCabangTrans = TBeli.IdMCabang AND TKorHutD.IdTrans = TBeli.IdTBeli)
        LEFT OUTER JOIN MGAPTRBeli TRBeli ON (TKorHutD.JenisTrans = 'R' AND TKorHutD.IdMCabangTrans = TRBeli.IdMCabang AND TKorHutD.IdTrans = TRBeli.IdTRBeli)
        LEFT OUTER JOIN MGGLMPrk MPrk ON (MPrk.IdMPrk = TKorHutD.IdMPrk AND MPrk.Periode = 0)
        LEFT OUTER JOIN MGAPMSUP MSup on (TBeli.IdMSup = MSup.IdMSup)
        WHERE TKorHut.TglTKorHut >= '${start} 00:00:00' and TKorHut.TglTKorHut <= '${end} 23:59:59' and TKorHut.Hapus = 0 and TKorHut.Void = 0`;
    return sql;
}

exports.queryKoreksiPiutang = async (companyid, start, end) => {
    var sql = ``;
    if (companyid == companyWI) { 

    }
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
        , IF(TKorPiut.JenisTKorPiut = 'D', TKorPiutD.JMLKOR, IF(TKorPiut.JenisTKorPiut = 'C', (TKorPiutD.JMLKOR* -1), TKorPiutD.JMLKOR)) As JmlKor
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
        WHERE TKorPiut.TglTKorPiut >= '${start} 00:00:00' and TKorPiut.TglTKorPiut <= '${end} 23:59:59' and TKorPiut.Hapus = 0 and TKorPiut.Void = 0`;
    return sql;
}

exports.queryJurnalMemo = async (companyid, start, end) => { 
    var sql = ``;

    if (companyid == companyWI) { 
        sql = `SELECT j.*, jurnal.TglTJurnal, jurnal.BuktiTJurnal, d.KdMPrk as KdMPrkD, d.NmMPrk as NmMPrkD, k.KdMPrk as KdMPrkK, k.NmMPrk as NmMPrkK
            FROM MGGLTJurnalD j 
            LEFT OUTER JOIN MGGLMPrk d ON (j.IdMPrkD = d.IdMPrk and d.Periode = 0)
            LEFT OUTER JOIN MGGLMPrk k ON (j.IdMPrkK = k.IdMPrk and k.Periode = 0)
            LEFT OUTER JOIN MGGLTJurnal jurnal on (jurnal.IdTJurnal = j.IdTJurnal)
            WHERE jurnal.TglTJurnal >= '${start} 00:00:00' and jurnal.TglTJurnal <= '${end} 23:59:59'`;
    }

    return sql;
}
