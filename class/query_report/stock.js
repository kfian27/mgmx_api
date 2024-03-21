const fun = require("../../mgmx");
var companyWI = fun.companyWI;

// tambahan IdMCabang, IdMGd, IdMBrg
exports.queryPosisiStock = async (companyid,tanggal) => {
  console.log('companywi', fun.companyWI);
  var sql = ``;  

  if (companyid == companyWI) {
    sql = `
      SELECT * FROM (
          SELECT MCabang.IdMCabang, MCabang.KdMCabang, MCabang.NmMCabang
              , MGd.IdMGd, MGd.KdMGd, MGd.NmMGd
              , MBrg.IdMBrg, MBrg.KdMBrg, MBrg.NmMBrg, MBrg.QtyMinStockGd, MBrg.QtyMinStockCabang
              , MJenisBrg.KdMJenisBrg, MJenisBrg.NmMJenisBrg
              , IF(IsNull(MStn1.KdMStn) = '', MStn1.KdMStn, '') as KdMStn1
              , IF(IsNull(MStn2.KdMStn) = '', MStn2.KdMStn, '') as KdMStn2
              , IF(IsNull(MStn3.KdMStn) = '', MStn3.KdMStn, '') as KdMStn3
              , IF(IsNull(MStn4.KdMStn) = '', MStn4.KdMStn, '') as KdMStn4
              , IF(IsNull(MStn5.KdMStn) = '', MStn5.KdMStn, '') as KdMStn5
              , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='UKURAN_BRG'),'') AS EditUKURAN_BRG
              , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='KMK'),'') AS EditKMK
              , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='MERK'),'') AS EditMERK
              , TablePosQty.PosQty
              , TablePosQty.PosValue
              , IF(TablePosQty.PosQty = 0, 0, TablePosQty.PosValue / TablePosQty.PosQty) as PosHrgStn
              , IF(TablePosQty.PosQty < 0, 0, IF(MBrg.IdMStn5 = 0, 0, floor(TablePosQty.PosQty) div (MBrg.IsiStn4 * MBrg.IsiStn3 * MBrg.IsiStn2 * MBrg.IsiStn1))) as Qty5
              , IF(TablePosQty.PosQty < 0, 0, IF(MBrg.IdMStn4 = 0, 0, floor(TablePosQty.PosQty) div (MBrg.IsiStn3 * MBrg.IsiStn2 * MBrg.IsiStn1))
              - floor(IF(MBrg.IdMStn5 = 0, 0, floor(TablePosQty.PosQty) div (MBrg.IsiStn4 * MBrg.IsiStn3 * MBrg.IsiStn2 * MBrg.IsiStn1)) * MBrg.IsiStn4))
              as Qty4
              , IF(TablePosQty.PosQty < 0, 0, IF(MBrg.IdMStn3 = 0, 0, floor(TablePosQty.PosQty) div (MBrg.IsiStn2 * MBrg.IsiStn1))
              - floor(IF(MBrg.IdMStn4 = 0, 0, floor(TablePosQty.PosQty) div (MBrg.IsiStn3 * MBrg.IsiStn2 * MBrg.IsiStn1)) * MBrg.IsiStn3))
              as Qty3
              , IF(TablePosQty.PosQty < 0, 0, IF(MBrg.IdMStn2 = 0, 0, floor(TablePosQty.PosQty) div (MBrg.IsiStn1))
              - floor(IF(MBrg.IdMStn3 = 0, 0, floor(TablePosQty.PosQty) div (MBrg.IsiStn2 * MBrg.IsiStn1)) * MBrg.IsiStn2))
              as Qty2
              , IF(TablePosQty.PosQty < 0, TablePosQty.PosQty, IF(coalesce(MBrg.IdMStn2, 0) = 0, TablePosQty.PosQty, TablePosQty.PosQty mod MBrg.IsiStn1))
              as Qty1
          FROM (
            SELECT IdMCabang, IdMGd, IdMBrg, Sum(QtyTotal) as PosQty, Sum(QtyTotal * HPP) as PosValue 
              FROM MGINLKartuStock LKartu
            WHERE TglTrans < '${tanggal} 23:59:59'
            GROUP BY IdMCabang, IdMGd, IdMBrg
          ) TablePosQty LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosQty.IdMCabang = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGSYMGd MGd ON (TablePosQty.IdMCabang = MGd.IdMCabang AND TablePosQty.IdMGd = MGd.IdMGd)
                        LEFT OUTER JOIN MGINMBrg MBrg ON (TablePosQty.IdMBrg = MBrg.IdMBrg)
                        LEFT OUTER JOIN MGINMJenisBrg MJenisBrg ON (MBrg.IdMJenisBrg = MJenisBrg.IdMJenisBrg)
                        LEFT OUTER JOIN MGINMStn MStn1 ON (MBrg.IdMStn1 = MStn1.IdMStn)
                        LEFT OUTER JOIN MGINMStn MStn2 ON (MBrg.IdMStn2 = MStn2.IdMStn)
                        LEFT OUTER JOIN MGINMStn MStn3 ON (MBrg.IdMStn3 = MStn3.IdMStn)
                        LEFT OUTER JOIN MGINMStn MStn4 ON (MBrg.IdMStn4 = MStn4.IdMStn)
                        LEFT OUTER JOIN MGINMStn MStn5 ON (MBrg.IdMStn5 = MStn5.IdMStn)
          WHERE MCabang.Hapus = 0
          AND MCabang.KdMCabang LIKE '%%'
          AND MCabang.NmMCabang LIKE '%%'
            AND MGd.Hapus = 0
            AND ((MGd.KdMGd LIKE '%%'
            AND MGd.NmMGd LIKE '%%')
            AND MGd.IdMGd <> 1000000)
            AND MBrg.Hapus = 0
            AND MBrg.KdMBrg LIKE '%%'
            AND MBrg.NmMBrg LIKE '%%'
            and MBrg.Reserved_int1 <> 4
            AND MJenisBrg.KdMJenisBrg LIKE '%%'
            AND MJenisBrg.NmMJenisBrg LIKE '%%'
            AND PosQty <> 0
          ORDER BY MCabang.KdMCabang, MGd.KdMGd, MJenisBrg.KdMJenisBrg, MBrg.NmMBrg
          ) as Tabel1 
          WHERE 
            EditUKURAN_BRG Like '%%'
          AND 
            EditKMK Like '%%'
          AND 
            EditMERK Like '%%'
      `;
  } else {
    console.log('ini massal');
    sql = `SELECT * FROM (
      SELECT MCabang.IdMCabang, MCabang.KdMCabang, MCabang.NmMCabang
          , MGd.IdMGd, MGd.KdMGd, MGd.NmMGd
          , MBrg.IdMBrg, MBrg.KdMBrg, MBrg.NmMBrg, MBrg.QtyMinStockGd, MBrg.QtyMinStockCabang
          , MBrg.Reserved_dec4 As BeratMBrg
          , MBrg.Keterangan
          , MJenisBrg.KdMJenisBrg, MJenisBrg.NmMJenisBrg
          , COALESCE(MStn1.KdMStn,'') AS KdMStn1
          , COALESCE(MStn2.KdMStn,'') AS KdMStn2
          , COALESCE(MStn3.KdMStn,'') AS KdMStn3
          , COALESCE(MStn4.KdMStn,'') AS KdMStn4
          , COALESCE(MStn5.KdMStn,'') AS KdMStn5
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='01'),'') AS Edit01
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='02'),'') AS Edit02
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='03'),'') AS Edit03
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='04'),'') AS Edit04
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='05'),'') AS Edit05
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='06'),'') AS Edit06
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='07'),'') AS Edit07
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='08'),'') AS Edit08
          , TablePosQty.PosQty
          , TablePosQty.PosValue
          , IF(TablePosQty.PosQty = 0, 0, TablePosQty.PosValue / TablePosQty.PosQty) as PosHrgStn
          , IF(TablePosQty.PosQty < 0, 0, IF(MBrg.IdMStn5 = 0, 0, TablePosQty.PosQty div (MBrg.IsiStn4 * MBrg.IsiStn3 * MBrg.IsiStn2 * MBrg.IsiStn1))) as Qty5
          , IF(TablePosQty.PosQty < 0, 0, IF(MBrg.IdMStn4 = 0, 0, TablePosQty.PosQty div (MBrg.IsiStn3 * MBrg.IsiStn2 * MBrg.IsiStn1))
            - IF(MBrg.IdMStn5 = 0, 0, TablePosQty.PosQty div (MBrg.IsiStn4 * MBrg.IsiStn3 * MBrg.IsiStn2 * MBrg.IsiStn1)) * MBrg.IsiStn4)
            as Qty4
          , IF(TablePosQty.PosQty < 0, 0, IF(MBrg.IdMStn3 = 0, 0, TablePosQty.PosQty div (MBrg.IsiStn2 * MBrg.IsiStn1))
            - IF(MBrg.IdMStn4 = 0, 0, TablePosQty.PosQty div (MBrg.IsiStn3 * MBrg.IsiStn2 * MBrg.IsiStn1)) * MBrg.IsiStn3)
            as Qty3
          , IF(TablePosQty.PosQty < 0, 0, IF(MBrg.IdMStn2 = 0, 0, TablePosQty.PosQty div (MBrg.IsiStn1))
            - IF(MBrg.IdMStn3 = 0, 0, TablePosQty.PosQty div (MBrg.IsiStn2 * MBrg.IsiStn1)) * MBrg.IsiStn2)
            as Qty2
          , IF(TablePosQty.PosQty < 0, TablePosQty.PosQty, IF(coalesce(MBrg.IdMStn2, 0) = 0, TablePosQty.PosQty, TablePosQty.PosQty mod MBrg.IsiStn1))
            as Qty1
          , HBT
      FROM (
        SELECT IdMCabang, IdMGd, IdMBrg, Sum(PosQty) as PosQty, Sum(PosValue) as PosValue, HBT 
        FROM (
          SELECT IdMCabang, IdMGd, IdMBrg, QtyTotal as PosQty, QtyTotal * HPP as PosValue, 0 as HBT 
          FROM MGINLKartuStock LKartu
          WHERE TglTrans < '${tanggal} 23:59:59'
        UNION ALL
          SELECT IdMCabang, IdMGd, IdMBrg, QtyTotal as PosQty,  HPP as PosValue, 0 as HBT 
          FROM MGINLKartuStock LKartu
          WHERE TglTrans < '${tanggal} 23:59:59'
            AND BuktiTrans Like '%Pembulatan%'
        )TblGabung
        GROUP BY IdMCabang, IdMGd, IdMBrg
      ) TablePosQty LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosQty.IdMCabang = MCabang.IdMCabang)
                    LEFT OUTER JOIN MGSYMGd MGd ON (TablePosQty.IdMCabang = MGd.IdMCabang AND TablePosQty.IdMGd = MGd.IdMGd)
                    LEFT OUTER JOIN MGINMBrg MBrg ON (TablePosQty.IdMBrg = MBrg.IdMBrg)
                    LEFT OUTER JOIN MGINMJenisBrg MJenisBrg ON (MBrg.IdMJenisBrg = MJenisBrg.IdMJenisBrg)
                    LEFT OUTER JOIN MGINMStn MStn1 ON (MBrg.IdMStn1 = MStn1.IdMStn)
                    LEFT OUTER JOIN MGINMStn MStn2 ON (MBrg.IdMStn2 = MStn2.IdMStn)
                    LEFT OUTER JOIN MGINMStn MStn3 ON (MBrg.IdMStn3 = MStn3.IdMStn)
                    LEFT OUTER JOIN MGINMStn MStn4 ON (MBrg.IdMStn4 = MStn4.IdMStn)
                    LEFT OUTER JOIN MGINMStn MStn5 ON (MBrg.IdMStn5 = MStn5.IdMStn)
      WHERE MCabang.Hapus = 0
        AND MGd.Hapus = 0
        AND ((MGd.KdMGd LIKE '%%'
        AND MGd.NmMGd LIKE '%%')
        OR MGd.IdMGd = 1000000)
        AND MBrg.Hapus = 0
        AND MBrg.KdMBrg LIKE '%%'
        AND MBrg.NmMBrg LIKE '%%'
        AND MBrg.Reserved_int1 <> 4
        AND MBrg.Reserved_int1 <> 2
        AND MBrg.Reserved_int1 <> 3
        AND PosQty <> 0
      ORDER BY MCabang.KdMCabang, MGd.KdMGd, MJenisBrg.KdMJenisBrg, MBrg.NmMBrg
      ) as Tabel1 
      WHERE 
        Edit01 Like '%%'
      AND 
        Edit02 Like '%%'
      AND 
        Edit03 Like '%%'
      AND 
        Edit04 Like '%%'
      AND 
        Edit05 Like '%%'
      AND 
        Edit06 Like '%%'
      AND 
        Edit07 Like '%%'
      AND 
        Edit08 Like '%%'
      `;
  }

    return sql;
}



exports.queryKartuStock = async (companyid, start, end, cabang, gudang, barang) => { 
  let qcabang = "";
  if (cabang != "") {
    qcabang = "AND MCabang.IdMCabang=" + cabang;
  }
  
  let qgudang = "";
  if (gudang != "") {
    qgudang = "AND MGd.IdMGd = " + gudang;
  }
  
  let qbarang = "";
  if (barang != "") {
    qbarang = "AND MBrg.IdMBrg = " + barang;
  }
  var sql = ``;
  if (companyid == companyWI) {
    sql = `Select * from (
      SELECT MCabang.KdMCabang
          , MCabang.NmMCabang
          , MGd.KdMGd
          , MGd.NmMGd
          , MBrg.KdMBrg
          , MBrg.NmMBrg
          , MBrg.KdMStn 
          , TableKartuStock.IdMBrg
          , TableKartuStock.IdMCabang
          , Urut
          , BuktiTrans
          , cast(TglTrans as datetime) as TglTrans
          , TableKartuStock.Keterangan, Saldo, QtyTotal
          , IF(Urut = 0, 0, IF(IF(IsNull(QtyTotal), 0, QtyTotal) > 0, IF(IsNull(QtyTotal), 0, QtyTotal), 0)) As Debit
          , IF(Urut = 0, 0, IF(IF(IsNull(QtyTotal), 0, QtyTotal) >= 0, 0, IF(IsNull(QtyTotal), 0, -QtyTotal))) As Kredit
          , IF(IsNull(HPP), 0, HPP) As HPP
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = 'UKURAN_BRG'), '') AS EditUKURAN_BRG
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = 'KMK'), '') AS EditKMK
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = 'MERK'), '') AS EditMERK
      FROM (
      SELECT IdMCabang, IdMGd, IdMBrg, 0 As Urut, 0 as JenisTrans, 0 as IdTrans, 0 as IdTransD, 000 As BuktiTrans, cast('${start} 00:00:00' as DateTime) As TglTrans, 0 As QtyTotal, sum(QtyTotal) As Saldo, 0 as HPP, 'Saldo Awal' As Keterangan FROM (
        SELECT IdMCabang, IdMGd, IdMBrg, QtyTotal FROM MGINLKartuStock where TglTrans < '${start} 00:00:00'
      ) TableSaldoAwal
      GROUP BY IdMCabang, IdMGd, IdMBrg
      UNION ALL
      SELECT IdMCabang, IdMGd, IdMBrg, 1 as Urut, JenisTrans, IdTrans, IdTransD, BuktiTrans, TglTrans, SUM(QtyTotal) AS QtyTotal, 0, HPP, Keterangan FROM MGINLKartuStock where TglTrans >= '${start} 00:00:00' and TglTrans <= '${end} 23:59:59' and IdMGd <> 1000000
       GROUP BY IdMCabang, IdMGd, IdMBrg, IdTrans, JenisTrans, HrgStn 
      UNION ALL
      SELECT IdMCabang, IdMGd, IdMBrg, IF (JenisTrans = 'ITM', 2, IF (JenisTrans = 'ITK', 3, 4)) AS Urut, JenisTrans, IdTrans, IdTransD, BuktiTrans, TglTrans, SUM(QtyTotal) AS QtyTotal, 0, HPP, Keterangan FROM MGINLKartuStock where TglTrans >= '${start} 00:00:00' and TglTrans <= '${end} 23:59:59' and IdMGd = 1000000
       GROUP BY IdMCabang, IdMGd, IdMBrg, IdTrans, JenisTrans, HrgStn
      ) TableKartuStock LEFT OUTER JOIN MGSYMCabang MCabang ON (TableKartuStock.IdMCabang = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGSYMGd MGd ON (TableKartuStock.IdMCabang = MGd.IdMCabang AND TableKartuStock.IdMGd = MGd.IdMGd)
                        LEFT OUTER JOIN MGINMBrg MBrg ON (TableKartuStock.IdMBrg = MBrg.IdMBrg)
      WHERE MCabang.Hapus = 0
        AND MCabang.KdMCabang LIKE '%%'
        AND MCabang.NmMCabang LIKE '%%'
        AND ((MGd.KdMGd LIKE '%%'
        AND MGd.NmMGd LIKE '%%')
        AND MGd.IdMGd <> 1000000)
        AND MBrg.KdMBrg LIKE '%%'
        AND MBrg.NmMBrg LIKE '%%'
        AND MBrg.Reserved_int1 <> 4 ${qcabang} ${qgudang} ${qbarang}
      ORDER BY TableKartuStock.IdMCabang
      , TableKartuStock.IdMGd
      , MBrg.KdMBrg, Urut, cast(TglTrans As Date), JenisTrans, IdTrans, IdTransD
      ) Tabel1
      WHERE 
        EditUKURAN_BRG Like '%%'
      AND 
        EditKMK Like '%%'
      AND 
        EditMERK Like '%%'
      `;
  } else {
    
    sql = `Select * from (
      SELECT MCabang.KdMCabang
          , MCabang.NmMCabang
          , MGd.KdMGd
          , MGd.NmMGd
          , MBrg.KdMBrg
          , MBrg.NmMBrg
          , TableKartuStock.IdMBrg
          , TableKartuStock.IdMCabang
          , Urut
          , TableKartuStock.BuktiTrans
          , IF(JenisTrans = 'ISA', 1,
            IF(JenisTrans = 'PBL', 2,
            IF(JenisTrans = 'IPY', 3,
            IF(JenisTrans = 'LSB', 3.5,
            IF(JenisTrans = 'LSH', 3.6,
            IF(JenisTrans = 'PRB', 4,
            IF(JenisTrans = 'IKA', 5,
            IF(JenisTrans = 'IKT', 6,
            IF(JenisTrans = 'ITK', 7,
            IF(JenisTrans = 'ITM', 8,
            IF(JenisTrans = 'PPB', 9,
            IF(JenisTrans = 'PPH', 10,
            IF(JenisTrans = 'RJL', 11,
            IF(JenisTrans = 'RJP', 12,
            IF(JenisTrans = 'RRJ', 13,
            IF(JenisTrans = 'ITM', 14,
            IF(JenisTrans = 'ITK', 15,
            16))))))))))))))))) as UrutTrans
          , cast(TglTrans as datetime) as TglTrans
          , TableKartuStock.TglCreate
          , TableKartuStock.Keterangan, Saldo, TableKartuStock.QtyTotal
          , IF(Urut = 0, 0, IF(IF(COALESCE(TableKartuStock.QtyTotal,0)=0, 0, TableKartuStock.QtyTotal) > 0, IF(COALESCE(TableKartuStock.QtyTotal,0)=0, 0, TableKartuStock.QtyTotal), 0)) As Debit
          , IF(Urut = 0, 0, IF(IF(COALESCE(TableKartuStock.QtyTotal,0)=0, 0, TableKartuStock.QtyTotal) >= 0, 0, IF(COALESCE(TableKartuStock.QtyTotal,0)=0, 0, -TableKartuStock.QtyTotal))) As Kredit
          , IF(COALESCE(TableKartuStock.HPP,0)=0, 0, TableKartuStock.HPP) As HPP
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = '01'), '') AS Edit01
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = '02'), '') AS Edit02
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = '03'), '') AS Edit03
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = '04'), '') AS Edit04
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = '05'), '') AS Edit05
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = '06'), '') AS Edit06
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = '07'), '') AS Edit07
          , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol AND MGol.Hapus = 0) where mdgol.idmbrg = MBrg.idmbrg AND mgol.Hapus = 0 and mgol.kdmgol = '08'), '') AS Edit08
      FROM (
      SELECT IdMCabang, IdMGd, IdMBrg, 0 As Urut, '' as JenisTrans, 0 as IdTrans, 0 as IdTransD, '' As BuktiTrans, cast('${start} 00:00:00' as DateTime) As TglTrans
            , 0 As QtyTotal, sum(QtyTotal) As Saldo, 0 as HPP, 'Saldo Awal' As Keterangan, Max(TglCreate) as TglCreate FROM (
        SELECT TblStock.IdMCabang, TblStock.IdMGd, TblStock.IdMBrg
              , TblStock.QtyTotal, TblStock.JenisTrans, IF(TblStock.TglCreate = '1900-01-01 00:00:00', TblStock.TglTrans, TblStock.TglCreate) AS TglCreate
        FROM MGINLKartuStock TblStock
        where TblStock.TglTrans < '${start} 00:00:00'
      ) TableSaldoAwal
      GROUP BY TableSaldoAwal.IdMCabang, TableSaldoAwal.IdMGd, TableSaldoAwal.IdMBrg
      UNION ALL
      SELECT TblStock.IdMCabang, TblStock.IdMGd, TblStock.IdMBrg, 1 as Urut, TblStock.JenisTrans, TblStock.IdTrans, TblStock.IdTransD, TblStock.BuktiTrans, TblStock.TglTrans
            , SUM(TblStock.QtyTotal) AS QtyTotal, 0, TblStock.HPP, TblStock.Keterangan, IF(TblStock.TglCreate = '1900-01-01 00:00:00', TblStock.TglTrans, TblStock.TglCreate) AS TglCreate
      FROM MGINLKartuStock TblStock 
      where TglTrans >= '${start} 00:00:00' and TglTrans <= '${end} 23:59:59' and TblStock.IdMGd <> 1000000
       GROUP BY TblStock.IdMCabang, TblStock.IdMGd, TblStock.IdMBrg, TblStock.IdTrans, TblStock.JenisTrans, TblStock.HrgStn 
      UNION ALL
      SELECT TblStock.IdMCabang, TblStock.IdMGd, TblStock.IdMBrg, IF (TblStock.JenisTrans = 'ITM', 2, IF (TblStock.JenisTrans = 'ITK', 3, 4)) AS Urut, TblStock.JenisTrans, TblStock.IdTrans, TblStock.IdTransD, TblStock.BuktiTrans, TblStock.TglTrans
            , SUM(TblStock.QtyTotal) AS QtyTotal, 0, TblStock.HPP, TblStock.Keterangan, IF(TblStock.TglCreate = '1900-01-01 00:00:00', TblStock.TglTrans, TblStock.TglCreate) AS TglCreate
      FROM MGINLKartuStock  TblStock
      where TglTrans >= '${start} 00:00:00' and TglTrans <= '${end} 23:59:59' and TblStock.IdMGd = 1000000
       GROUP BY TblStock.IdMCabang, TblStock.IdMGd, TblStock.IdMBrg, TblStock.IdTrans, TblStock.JenisTrans, TblStock.HrgStn
      ) TableKartuStock LEFT OUTER JOIN MGSYMCabang MCabang ON (TableKartuStock.IdMCabang = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGSYMGd MGd ON (TableKartuStock.IdMCabang = MGd.IdMCabang AND TableKartuStock.IdMGd = MGd.IdMGd)
                        LEFT OUTER JOIN MGINMBrg MBrg ON (TableKartuStock.IdMBrg = MBrg.IdMBrg)
      WHERE MCabang.Hapus = 0
      AND MCabang.KdMCabang LIKE '%%'
      AND MCabang.NmMCabang LIKE '%%'
      AND ((MGd.KdMGd LIKE '%%'
      AND MGd.NmMGd LIKE '%%')
      OR MGd.IdMGd = 1000000)
      AND MBrg.KdMBrg LIKE '%%'
      AND MBrg.NmMBrg LIKE '%%'
      AND MBrg.Reserved_int1 <> 4
      AND MBrg.Reserved_int1 <> 2
      AND MBrg.Reserved_int1 <> 3
      ${qcabang} ${qgudang} ${qbarang}
      ORDER BY TableKartuStock.IdMCabang
      , TableKartuStock.IdMGd
      , MBrg.KdMBrg, cast(TglTrans As Date), Urut, UrutTrans, TableKartuStock.TglCreate, JenisTrans, TableKartuStock.IdTrans, TableKartuStock.IdTransD
      ) Tabel1
      WHERE 
        Edit01 Like '%%'
      AND 
        Edit02 Like '%%'
      AND 
        Edit03 Like '%%'
      AND 
        Edit04 Like '%%'
      AND 
        Edit05 Like '%%'
      AND 
        Edit06 Like '%%'
      AND 
        Edit07 Like '%%'
      AND 
        Edit08 Like '%%'
    `
  }
  
  
  return sql;
}