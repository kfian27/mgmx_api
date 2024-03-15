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
        sql = `SELECT 'Customer' as LabelTitleCust
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
                    , TJualLainDPak.Harga AS HargaPak
                    , TJualLainDEx.Harga AS HargaEx
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
            FROM MGARTJual TJual
                    LEFT OUTER JOIN MGARTJualLain TJualLain ON (TJual.IdMCabang = TJualLain.IdMCabang AND TJual.BuktiTJual = TJualLain.BuktiAsli AND TJualLain.Hapus = 0 AND TJualLain.Void = 0)
                    LEFT OUTER JOIN MGARTJualLainD TJualLainDPak ON (TJualLain.IdMCabang = TJualLainDPak.IdMCabang AND TJualLain.IdTJualLain = TJualLainDPak.IdTJualLain AND MOD(TJualLainDPak.IdTJualLainD, 2) = 0)
                    LEFT OUTER JOIN MGARTJualLainD TJualLainDEx ON (TJualLain.IdMCabang = TJualLainDEx.IdMCabang AND TJualLain.IdTJualLain = TJualLainDEx.IdTJualLain AND MOD(TJualLainDEx.IdTJualLainD, 2) <> 0)
                    LEFT OUTER JOIN MGSYMCabang MCabang ON (TJual.IdMCabang = MCabang.IdMCabang)
                    LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJual.IdMCabangMCust AND MCust.IdMCust = TJual.IdMCust)
                    LEFT OUTER JOIN MGARMMember MMember ON (MMember.IdMMember = TJual.IdMMember)
                    LEFT OUTER JOIN MGSYMUser MUser ON (MUser.IdMCabang = TJual.IdMCabang AND MUser.IdMUser = TJual.IdMUserUpdate)
                    LEFT OUTER JOIN MGSVTPengerjaanService Sv ON (Sv.IdMCabang = TJual.IdMCabang and Sv.IdTPengerjaanService = TJual.IdTPengerjaanService)
                    LEFT OUTER JOIN MGAPMSup Sup ON (Sup.IdMSup = Sv.IdMSup)
            WHERE (TJual.TglTJual >= '${start} 00:00:00' AND TJual.TglTJual < '${end} 00:00:00')
                AND TJual.Hapus = 0
                AND TJual.Void = 0
                AND TJual.AprtBulan <> 1
                AND TJual.IdTRJual = 0
                AND TJual.AprtBulan <> 2
                ${where}
            GROUP BY TJual.IdTJual
            ORDER BY MCabang.KdMCabang, TJual.TglTJual, TJual.BuktiTJual, MCust.KdMCust`
    }else{
        sql = ``
    }
    return sql;
}

exports.queryDetail = async (companyid,start,end,cabang,supplier,barang, group) => {
    let where = "";
    if (cabang != "") {
        where += "AND MCabang.IdMCabang =" + cabang;
    }
    if (supplier != "") {
        where += "AND TBeli.IdMSup =" + supplier;
    }
    if (barang != "") {
        where += "AND MBrg.IdMBrg = " + barang;
    }

    var orderby = "ORDER BY";
    if(group == "cabang"){
        orderby += " KdMCabang";
    }else if(group == "supplier"){
        orderby += " IdMSup";
    }else{
        orderby += " KdMBrg";
    }
    orderby += ", TglTBeli DESC"

    var sql = "";
    if (companyid == companyWI) {
        sql = `Select * from (
            select MCabang.KdMCabang, MCabang.NmMCabang, MSup.KdMSup, MSup.NmMSup, TBeli.IdMSup
                 , TBeli.IdTBeli, TBeli.IdMCabang
                 , Date(TBeli.TglTBeli) As TglTBeli, TBeli.BuktiTBeli, TBeli.BuktiAsli
                 , TBeli.Bruto, TBeli.BrutoNon
                 , TBeli.DiscP
                 , TBeli.DiscV
                 , TBeli.PPNP
                 , TBeli.PPNV
                 , TBeli.Netto
                 , (TBeli.DiscP) as DiscNonP
                 , (TBeli.DiscV) as DiscNonV
                 , (TBeli.PPNP) as PPNNonP
                 , (TBeli.PPNV) as PPNNonV
                 , Time(TBeli.TglUpdate) As Waktu
                 , IF(TBeli.IdMKas <> 0, 'Kas', NULL) As StatusKas
                 , IF(TBeli.IdMKas <> 0, ':', NULL) As TandaKas
                 , IF(TBeli.IdMKas <> 0, Concat(MKas.KdMKas, '/', MKas.NmMKas), NULL) As NamaKas
                 , TBeli.JmlBayarTunai
                 , IF(TBeli.JmlBayarKredit > 0, 'Dibayar Kredit', NULL) As StatusBayarKredit
                 , IF(TBeli.JmlBayarKredit > 0, ':', NULL) As TandaKredit
                 , IF(TBeli.JmlBayarKredit > 0, TBeli.JmlBayarKredit, NULL) As JmlBayarKredit
                 , IF(TBeli.JmlBayarKredit > 0, 'Jatuh Tempo', NULL) As StatusJatuhTempo
                 , IF(TBeli.JmlBayarKredit > 0, Date(TBeli.TglJTHut), NULL) As TglJTHut
                 , TBeli.NoInvoicePabrik
                 , TBeliD.QtyTotal
                 , TBeliD.HrgStn
                 , TBeliD.DiscP As DiscPD
                 , TBeliD.DiscV As DiscVD
                 , TBeliD.SubTotal
                 , TBeliD.Baris
                 , MBrg.IdMBrg, MBrg.KdMBrg, MBrg.NmMBrg, MBrg.Reserved_int1
                 , MGd.KdMGd, MGd.NmMGd
                 ,COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='UKURAN_BRG'),'') AS EditUKURAN_BRG
                 ,COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='KMK'),'') AS EditKMK
                 ,COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='MERK'),'') AS EditMERK
                 , IF(TBeliD.Qty1=0, NULL, TBeliD.Qty1) As Qty1, IF(TBeliD.Qty1=0, NULL, g1.NmMStn) As NmMStn1
                 , IF(TBeliD.Qty2=0, NULL, TBeliD.Qty2) As Qty2, IF(TBeliD.Qty2=0, NULL, g2.NmMStn) As NmMStn2
                 , IF(TBeliD.Qty3=0, NULL, TBeliD.Qty3) As Qty3, IF(TBeliD.Qty3=0, NULL, g3.NmMStn) As NmMStn3
                 , IF(TBeliD.Qty4=0, NULL, TBeliD.Qty4) As Qty4, IF(TBeliD.Qty4=0, NULL, g4.NmMStn) As NmMStn4
                 , IF(TBeliD.Qty5=0, NULL, TBeliD.Qty5) As Qty5, IF(TBeliD.Qty5=0, NULL, g5.NmMStn) As NmMStn5
                 , TBeliD.QtyTotal * MBrg.Reserved_dec2 as Kg
                 , TBeliD.QtyTotal * MBrg.Reserved_dec2 * MBrg.Reserved_dec3 as Stn2
                 , COALESCE((IF(TBeli.JmlBayarKredit > 0, TBeli.JmlBayarKredit, NULL)),TBeli.JmlBayarTunai) as bayar
            FROM MGAPTBeliD TBeliD
                 LEFT OUTER JOIN MGAPTBeli TBeli ON (TBeliD.IdTBeli = TBeli.IdTBeli AND TBeliD.IdMCabang = TBeli.IdMCabang)
                 LEFT OUTER JOIN MGKBMKas MKas ON (MKas.IdMKas = TBeli.IdMKas AND MKas.IdMCabang = TBeli.IdMCabang)
                 LEFT OUTER JOIN MGSYMCabang MCabang ON (MCabang.IdMCabang=TBeli.IdMCabang)
                 LEFT OUTER JOIN MGAPMSup MSup ON (MSup.IdMSup=TBeli.IdMSup)
                 LEFT OUTER JOIN MGINMBrg MBrg ON (MBrg.IdMBrg=TBeliD.IdMBrg) 
                 LEFT OUTER JOIN MGINMStn g1 ON (g1.IdMStn=MBrg.IdMStn1)
                 LEFT OUTER JOIN MGINMStn g2 ON (g2.IdMStn=MBrg.IdMStn2)
                 LEFT OUTER JOIN MGINMStn g3 ON (g3.IdMStn=MBrg.IdMStn3)
                 LEFT OUTER JOIN MGINMStn g4 ON (g4.IdMStn=MBrg.IdMStn4)
                 LEFT OUTER JOIN MGINMStn g5 ON (g5.IdMStn=MBrg.IdMStn5)
                 LEFT OUTER JOIN MGSYMGd MGd ON (MGd.IdMCabang = TBeliD.IdMCabang AND MGd.IdMGd = TBeliD.IdMGd)
              WHERE MCabang.Hapus = 0
                AND MCabang.Aktif = 1
                AND MSup.Hapus = 0
                AND MSup.Aktif = 1
                AND MSup.KdMSup Like '%%'
                AND MSup.NmMSup Like '%%'
                AND MBrg.Hapus = 0
                AND MBrg.Aktif = 1
                AND MBrg.KdMBrg Like '%%'
                AND MBrg.NmMBrg Like '%%'
                AND (TBeli.TglTBeli >= '${start} 00:00:00' and TBeli.TglTBeli <= '${end} 23:59:59')
                AND TBeli.Hapus = 0
                AND TBeli.Void = 0
                AND TBeli.BuktiTBeli <> '' 
                AND TBeli.IdMUserCreate >= 0
                ${where}
            ORDER BY MCabang.KdMCabang, TBeli.TglTBeli, TBeli.BuktiTBeli, MSup.KdMSup
            ) as Tabel1 
             WHERE 
               EditUKURAN_BRG Like '%%'
             AND 
               EditKMK Like '%%'
             AND 
               EditMERK Like '%%'
            ${orderby}`;
    }else {
        sql = ``;
    }
    return sql;
}