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
        sql = ``
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
        orderby += " KdMCabang ASC";
    }else if(group == "supplier"){
        orderby += " IdMSup ASC";
    }else{
        orderby += " KdMBrg ASC";
    }
    orderby += ", TglTBeli ASC, BuktiTBeli, KdMBrg ASC"

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
                 , (TBeliD.DiscV*TBeliD.QtyTotal) As DiscVD
                 , TBeliD.SubTotal
                 , TBeliD.Baris
                 , MBrg.IdMBrg, MBrg.KdMBrg, MBrg.NmMBrg, MBrg.Reserved_int1
                 , MGd.KdMGd, MGd.NmMGd
                 , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='UKURAN_BRG'),'') AS EditUKURAN_BRG
                 , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='KMK'),'') AS EditKMK
                 , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='MERK'),'') AS EditMERK
                 , IF(TBeliD.Qty1=0, NULL, TBeliD.Qty1) As Qty1, IF(TBeliD.Qty1=0, NULL, g1.NmMStn) As NmMStn1
                 , IF(TBeliD.Qty2=0, NULL, TBeliD.Qty2) As Qty2, IF(TBeliD.Qty2=0, NULL, g2.NmMStn) As NmMStn2
                 , IF(TBeliD.Qty3=0, NULL, TBeliD.Qty3) As Qty3, IF(TBeliD.Qty3=0, NULL, g3.NmMStn) As NmMStn3
                 , IF(TBeliD.Qty4=0, NULL, TBeliD.Qty4) As Qty4, IF(TBeliD.Qty4=0, NULL, g4.NmMStn) As NmMStn4
                 , IF(TBeliD.Qty5=0, NULL, TBeliD.Qty5) As Qty5, IF(TBeliD.Qty5=0, NULL, g5.NmMStn) As NmMStn5
                 , TBeliD.QtyTotal * MBrg.Reserved_dec2 as Kg
                 , TBeliD.QtyTotal * MBrg.Reserved_dec2 * MBrg.Reserved_dec3 as Stn2
                 , TBeli.JmlBayarTunai as bayar
                 , ((TBeliD.HrgStn * TBeliD.QtyTotal) - (TBeliD.DiscV * TBeliD.QtyTotal)) as dpp
                 , (select SUM(JmlBayar)
                    from mgaptbhutd m join mgaptbhut m2 on m.IdMCabang = m2.IdMCabang and m.IdTBHut = m2.IdTBHut 
                    where m.JenisTrans = 'T' and m2.Hapus = 0 and m2.Void = 0 and m.IdTrans = TBeli.IdTBeli) as total_bayar
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
        sql = `Select * from (
            select MCabang.KdMCabang, MCabang.NmMCabang, MSup.KdMSup, MSup.NmMSup, MSup.IdMSup
                 , TBeli.IdTBeli, TBeli.IdMCabang
                 , Date(TBeli.TglTBeli) As TglTBeli, TBeli.BuktiTBeli, TBeli.BuktiAsli
                 , TBeli.Bruto, TBeli.BrutoNon
                 , TBeli.DiscP
                 , TBeli.DiscV
                 , TBeli.PPNP
                 , TBeli.PPNV
                 , TBeli.PPHP
                 , TBeli.PPHV
                 , TBeli.Netto
                 , (TBeli.DiscP) as DiscNonP
                 , (TBeli.DiscV) as DiscNonV
                 , (TBeli.PPNP) as PPNNonP
                 , (TBeli.PPNV) as PPNNonV
                 , (TBeli.PPHP) as PPHNonP
                 , (TBeli.PPHV) as PPHNonV
                 , Time(TBeli.TglUpdate) As Waktu
                 , IF(TBeli.IdMKas <> 0, 'Kas', NULL) As StatusKas
                 , IF(TBeli.IdMKas <> 0, ':', NULL) As TandaKas
                 , IF(TBeli.IdMKas <> 0, Concat(MKas.KdMKas, '/', MKas.NmMKas), NULL) As NamaKas
                 , TBeli.JmlBayarTunai
                 , IF(TBeli.JmlBayarKredit > 0, 'Dibayar Kredit', '') As StatusBayarKredit
                 , IF(TBeli.JmlBayarKredit > 0, ':', NULL) As TandaKredit
                 , IF(TBeli.JmlBayarKredit > 0, TBeli.JmlBayarKredit, NULL) As JmlBayarKredit
                 , IF(TBeli.JmlBayarKredit > 0, 'Jatuh Tempo', '') As StatusJatuhTempo
                 , IF(TBeli.JmlBayarKredit > 0, Date(TBeli.TglJTHut), NULL) As TglJTHut
                 , TBeli.NoInvoicePabrik
                 , TBeli.NilaiPembulatanDecimal
                 , TBeliD.QtyTotal
                 , TBeliD.HrgStn
                 , TBeliD.DiscP As DiscPD
                 , (TBeliD.DiscV*TBeliD.QtyTotal) As DiscVD
                 , TBeliD.SubTotal
                 , TBeliD.PPNV as PPNVEcer
                 , TBeliD.Baris
                 , MBrg.IdMBrg, MBrg.KdMBrg, MBrg.NmMBrg, MBrg.Reserved_int1
                 , MGd.KdMGd, MGd.NmMGd
                 , COALESCE(MProject.KdMProject, '') as KdMProject, COALESCE(MProject.NmMProject,'') as NmmProject
                 , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='GOL1'),'') AS EditGOL1
                 , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='GOL2'),'') AS EditGOL2
                 , IF(TBeliD.Qty1=0, NULL, TBeliD.Qty1) As Qty1, IF(TBeliD.Qty1=0, NULL, g1.NmMStn) As NmMStn1
                 , IF(TBeliD.Qty2=0, NULL, TBeliD.Qty2) As Qty2, IF(TBeliD.Qty2=0, NULL, g2.NmMStn) As NmMStn2
                 , IF(TBeliD.Qty3=0, NULL, TBeliD.Qty3) As Qty3, IF(TBeliD.Qty3=0, NULL, g3.NmMStn) As NmMStn3
                 , IF(TBeliD.Qty4=0, NULL, TBeliD.Qty4) As Qty4, IF(TBeliD.Qty4=0, NULL, g4.NmMStn) As NmMStn4
                 , IF(TBeliD.Qty5=0, NULL, TBeliD.Qty5) As Qty5, IF(TBeliD.Qty5=0, NULL, g5.NmMStn) As NmMStn5
                 , TBeliD.QtyTotal * MBrg.Reserved_dec2 as Kg
                 , TBeliD.QtyTotal * MBrg.Reserved_dec2 * MBrg.Reserved_dec3 as Stn2
                 , TBeli.PBBKBP, TBeli.PBBKBV, TBeli.PPH22P, TBeli.PPH22V
                 , TBeli.JmlBayarTunai as bayar
                 , ((TBeliD.HrgStn * TBeliD.QtyTotal) - (TBeliD.DiscV * TBeliD.QtyTotal)) as dpp                 
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
                 LEFT OUTER JOIN MGKTMProject MPRoject ON (TBelid.IdMCabangMProject = MProject.IdMCabang and TBelid.IdMProject = MProject.IdMProject)
              WHERE MCabang.Hapus = 0
                AND MSup.Hapus = 0
                AND MSup.KdMSup Like '%%'
                AND MSup.NmMSup Like '%%'
                AND MBrg.Hapus = 0
                AND MBrg.KdMBrg Like '%%'
                AND MBrg.NmMBrg Like '%%'
                AND (TBeli.TglTBeli >= '${start} 00:00:00' and TBeli.TglTBeli <= '${end} 23:59:59')
                AND TBeli.Hapus = 0
                AND TBeli.Void = 0
               AND TBeli.BuktiTBeli <> '' 
             AND TBeli.IdMUserCreate >= 0
             ${where}
            ORDER BY MCabang.KdMCabang, TBeli.TglTBeli, MSup.KdMSup, TBeli.BuktiTBeli
            ) as Tabel1 
             WHERE 
               EditGOL1 Like '%%'
             AND 
               EditGOL2 Like '%%'
            ${orderby}`;
    }
    return sql;
}