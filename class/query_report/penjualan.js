const fun = require("../../mgmx");
var companyWI = fun.companyWI;

exports.querySummary = async (companyid,start,end,cabang,customer,barang) => {
    var sql = "";
    let where = "";
    if (cabang != "") {
        where += "AND MCabang.IdMCabang=" + cabang;
    }
    let qcustomer = "";
    if (customer != "") {
        where += "AND MCust.KdMCust=" + customer;
    }
    let qbarang = "";
    if (barang != "") {
        where += "AND MBrg.IdMBrg = " + barang;
    }
    if (companyid == companyWI) {
        sql = `SELECT * FROM (
            SELECT 'Customer' as LabelTitleCust
                 , MCabang.KdMCabang, MCabang.NmMCabang, MCust.KdMCust, MCust.NmMCust, MCust.IdMCust
                 , MMember.KdMMember, MMember.NmMMember
                 , IF(TJual.IdMCust = 0, CONCAT(MMember.NmMMember, ' (', MMember.KdMMember, ')'),
                   CONCAT(MCust.NmMCust, ' (', IF(TJual.IdMMember = 0, MCust.KdMCust, MMember.KdMMember),')')) AS namacust
                 , TJual.IdMCabang, TJual.IdTJual as IdTJualPOS
                 , Date(TJual.TglTJual) As TglTJualPOS, Time(TJual.TglUpdate) As Waktu
                 , IF(TJual.AprtBulan <> 2, TJual.BuktiTJual, TJual.NoBuktiAmbilService) as BuktiTJualPOS
                 , 0 as IdTModAwalKasir
                 , Date(TJual.TglTJual) As TglTModAwalKasir
                 , 'User' As StatusKasir
                 , MUser.KdMUser, MUser.NmMUser
                 , TJual.Bruto
                 , 'Disc(%)' As StatusBiaya
                 , ':' As TandaBiaya
                 , TJual.DiscP As Biaya
                 , TJual.DiscP As DiscP
                 , TJual.DiscV
                 , 'PPN(%)' As StatusPPN
                 , ':' As TandaPPN
                 , TJual.PPNP As PPNP
                 , TJual.PPNV
                 ,'Pengepakan' AS StatusEkspedisi
                 , ':' AS TandaEkspedisi
                 , Coalesce(TJualLain.Netto, 0) AS Ekspedisi
                 , TJual.Netto + Coalesce(TJualLain.Netto, 0) As Netto
                 , TJual.Netto + Coalesce(TJualLain.Netto, 0) AS NettoEkspedisi
                 , IF(TJual.JmlBayarKartu1 > 0, 'Dibayar Kartu 1', NULL) AS StatusBayarKartu
                 , IF(TJual.JmlBayarKartu1 > 0, ':', NULL) AS TandaKartu
                 , IF(TJual.JmlBayarKartu1 > 0, TJual.JmlBayarKartu1, NULL) AS Kartu
                 , IF(TJual.JmlBayarKartu2 > 0, 'Dibayar Kartu 2', NULL) As StatusBiayaKartu
                 , IF(TJual.JmlBayarKartu2 > 0, ':', NULL) As TandaBiayaKartu
                 , IF(TJual.JmlBayarKartu2 > 0, TJual.JmlBayarKartu2, NULL) As BiayaKartu
                 , IF(TJual.JmlBayarKartu3 > 0, 'Dibayar Kartu 3', NULL) As StatusKartu3
                 , IF(TJual.JmlBayarKartu3 > 0, ':', NULL) As TandaKartu3
                 , IF(TJual.JmlBayarKartu3 > 0, TJual.JmlBayarKartu3, NULL) As JmlBayarKartu
                 , TJual.JmlBayarTunai + TJual.JmlBayarDeposit As JmlBayarTunai
                 , IF(TJual.JmlBayarKredit > 0, 'Dibayar Kredit', NULL) As StatusBayarKredit
                 , IF(TJual.JmlBayarKredit > 0, ':', NULL) As TandaKredit
                 , IF((TJual.JmlBayarKredit + Coalesce(TJualLain.Netto, 0)) > 0, TJual.JmlBayarKredit + Coalesce(TJualLain.Netto, 0), NULL) As JmlBayarKredit
                 , IF(TJual.JmlBayarKredit > 0, 'Jatuh Tempo', NULL) As StatusJatuhTempo
                 , IF(TJual.JmlBayarKredit > 0, ':', NULL) As TandaJatuhTempo
                 , IF(TJual.JmlBayarKredit > 0, TglJTPiut, NULL) As TglJTPiut
                 , NULL As StatusKembali
                 , NULL As Kembali
                 , Sup.NmMSup as NmMTeknisi, Sup.KdMSup as KdMTeknisi
                 , MGd.KdMGd, MGd.NmMGd
                 , MBrg.KdMBrg
                 , MBrg.NmMBrg
                 , TJualD.IdMBrg, MBrg.Reserved_int1
                 , IF(TJualD.Qty1<=0, NULL, TJualD.Qty1) As Qty1, IF(TJualD.Qty1<=0, NULL, g1.NmMStn) As NmMStn1
                 , IF(TJualD.Qty2<=0, NULL, TJualD.Qty2) As Qty2, IF(TJualD.Qty2<=0, NULL, g2.NmMStn) As NmMStn2
                 , IF(TJualD.Qty3<=0, NULL, TJualD.Qty3) As Qty3, IF(TJualD.Qty3<=0, NULL, g3.NmMStn) As NmMStn3
                 , IF(TJualD.Qty4<=0, NULL, TJualD.Qty4) As Qty4, IF(TJualD.Qty4<=0, NULL, g4.NmMStn) As NmMStn4
                 , IF(TJualD.Qty5<=0, NULL, TJualD.Qty5) As Qty5, IF(TJualD.Qty5<=0, NULL, g5.NmMStn) As NmMStn5
                 , TJualD.QtyTotal
                 , TJualD.HrgStn, TJualD.DiscP As DiscPDetail
                 , TJualD.SubTotal
                 , TJualD.Keterangan
                 , TJualD.QtyTotal * MBrg.Reserved_dec2 as Kg
                 , TJualD.QtyTotal * MBrg.Reserved_dec2 * MBrg.Reserved_dec3 as Stn2
                 , TJualD.Baris
                 , COALESCE(TJual.KeteranganInternal,'') as KeteranganInternal
                 , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='UKURAN_BRG'),'') AS EditUKURAN_BRG
                 , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='KMK'),'') AS EditKMK
                 , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='MERK'),'') AS EditMERK
            FROM MGARTJualD TJualD
                 LEFT OUTER JOIN MGARTJual TJual ON (TJualD.IdMCabang = TJual.IdMCabang AND TJualD.IdTJual = TJual.IdTJual)
                 LEFT OUTER JOIN MGARTJualLain TJualLain ON (TJual.IdMCabang = TJualLain.IdMCabang AND TJual.BuktiTJual = TJualLain.BuktiAsli AND TJualLain.Hapus = 0 AND TJualLain.Void = 0)
                 LEFT OUTER JOIN MGSYMCabang MCabang ON (TJual.IdMCabang = MCabang.IdMCabang)
                 LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJual.IdMCabangMCust AND MCust.IdMCust = TJual.IdMCust)
                 LEFT OUTER JOIN MGARMMember MMember ON (MMember.IdMMember = TJual.IdMMember)
                 LEFT OUTER JOIN MGSYMUser MUser ON (MUser.IdMCabang = TJual.IdMCabang AND MUser.IdMUser = TJual.IdMUserUpdate)
                 LEFT OUTER JOIN MGSVTPengerjaanService Sv ON (Sv.IdMCabang = TJual.IdMCabang and Sv.IdTPengerjaanService = TJual.IdTPengerjaanService)
                 LEFT OUTER JOIN MGAPMSup Sup ON (Sup.IdMSup = Sv.IdMSup)
                 LEFT OUTER JOIN MGSYMGd MGd ON (MGd.IdMCabang = TJualD.IdMCabang AND MGd.IdMGd = TJualD.IdMGd)
                 LEFT OUTER JOIN MGINMBrg MBrg ON (MBrg.IdMBrg=TJualD.IdMBrg)
                 LEFT OUTER JOIN MGINMStn g1 ON (g1.IdMStn=MBrg.IdMStn1)
                 LEFT OUTER JOIN MGINMStn g2 ON (g2.IdMStn=MBrg.IdMStn2)
                 LEFT OUTER JOIN MGINMStn g3 ON (g3.IdMStn=MBrg.IdMStn3)
                 LEFT OUTER JOIN MGINMStn g4 ON (g4.IdMStn=MBrg.IdMStn4)
                 LEFT OUTER JOIN MGINMStn g5 ON (g5.IdMStn=MBrg.IdMStn5)
            WHERE MCabang.KdMCabang Like '%AIH%'
              AND MCabang.NmMCabang Like '%ALAM INDAH HARMONI%'
              AND MCust.KdMCust Like '%%'
              AND MCust.NmMCust Like '%%'
              AND MBrg.KdMBrg Like '%%'
              AND MBrg.NmMBrg Like '%%'
              AND MUser.KdMUser Like '%%'
              AND MUser.NmMUser Like '%%'
              AND (TJual.TglTJual >= '${start} 00:00:00' AND TJual.TglTJual < '${end} 00:00:00')
              AND TJual.Hapus = 0
              AND TJual.Void = 0
              AND TJual.AprtBulan <> 1
              AND TJual.IdTRJual = 0
             AND TJual.AprtBulan <> 2
            ORDER BY MCabang.KdMCabang, TJual.TglTJual, TJual.BuktiTJual, MCust.KdMCust
            ) As Table1
             WHERE 
               (EditUKURAN_BRG Like '%%' OR EditUKURAN_BRG Is Null)
             AND 
               (EditKMK Like '%%' OR EditKMK Is Null)
             AND 
               (EditMERK Like '%%' OR EditMERK Is Null) ${where}`;
    }else{
        sql = ``
    }
    return sql;
}


exports.queryDetail = async (companyid,start,end,cabang,customer,barang,group) => {
    let where = "";
    if (cabang != "") {
        where += "AND MCabang.IdMCabang=" + cabang;
    }
    let qcustomer = "";
    if (customer != "") {
        where += "AND MCust.KdMCust=" + customer;
    }
    let qbarang = "";
    if (barang != "") {
        where += "AND MBrg.IdMBrg = " + barang;
    }
    var sql = "";
    if (companyid == companyWI) {
        sql = ``;
    }else {
        sql = ``;
    }
    return sql;
}