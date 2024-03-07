exports.queryPosisiStockWI = async (tanggal) => {
    var sql = `
    SELECT * FROM (
        SELECT MCabang.KdMCabang, MCabang.NmMCabang
            , MGd.KdMGd, MGd.NmMGd
            , MBrg.KdMBrg, MBrg.NmMBrg
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
          WHERE TglTrans < '${tanggal} 00:00:00'
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
          AND MGd.IdMGd <> 1000000)
        AND  MGd.IdMGd in (select IdMGd from mgsymusermGd where idmuser=1 and idmcabangmuser=0 and idmcabangmGd=idmcabangmuser)
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

    return sql;
}