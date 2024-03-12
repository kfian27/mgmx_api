const fun = require("../../mgmx");
var companyWI = fun.companyWI;

exports.queryPosisiBank = async (companyid,tanggal) => {
    var sql = "";
    if (companyid == companyWI) {
      sql = `SELECT MCabang.KdMCabang,
              MCabang.NmMCabang, 
              MCabang.Aktif,
              MRek.KdMRek, 
              MRek.NmMRek, 
              MRek.Aktif, 
              TablePosRek.PosRek,
              MBank.NMMBANK
            FROM (
                SELECT TransAll.IdMCabang, IdMRek, Sum(JmlRek) as PosRek FROM (
                    Select k.TglTrans, k.IdMCabang, k.IdMRek, k.JmlRek FROM MGKBLKartuBank k
                    UNION ALL
                    SELECT '${tanggal} 00:00:00' as TglTrans, IdMCabang, IdMRek, 0 as JmlRek FROM MGKBMRek
                ) TransAll
                WHERE TglTrans <= '${tanggal} 23:59:59'
                GROUP BY TransAll.IdMCabang, IdMRek
            ) TablePosRek 
            LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosRek.IdMCabang = MCabang.IdMCabang)
            LEFT OUTER JOIN MGKBMRek MRek ON (TablePosRek.IdMCabang = MRek.IdMCabang AND TablePosRek.IdMRek = MRek.IdMRek)
            LEFT OUTER JOIN MGKBMBANK MBank on MRek.IDMBANK = MBank.IDMBANK
            WHERE MCabang.Hapus = 0
            AND MCabang.Aktif = 1
            AND MRek.Hapus = 0
            AND MRek.Aktif = 1
            AND MCabang.KdMCabang LIKE '%AIH%'
            AND MCabang.NmMCabang LIKE '%ALAM INDAH HARMONI%'
            AND MRek.KdMRek LIKE '%%'
            AND MRek.NmMRek LIKE '%%'
            AND PosRek <> 0
            ORDER BY MCabang.KdMCabang, MRek.NmMRek`;
    }else{
      sql = `SELECT MCabang.KdMCabang, MCabang.NmMCabang, MCabang.Aktif
                , MRek.KdMRek, MRek.NmMRek, MRek.Aktif
                , TablePosRek.PosRek
            FROM (
            SELECT TransAll.IdMCabang, IdMRek, Sum(JmlRek) as PosRek FROM (
            Select k.TglTrans, k.IdMCabang, k.IdMRek, k.JmlRek FROM MGKBLKartuBank k
            UNION ALL
            SELECT '${tanggal} 00:00:00' as TglTrans, IdMCabang, IdMRek, 0 as JmlRek FROM MGKBMRek
            ) TransAll
            WHERE TglTrans '${tanggal} 23:59:59'
            GROUP BY TransAll.IdMCabang, IdMRek
            ) TablePosRek LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosRek.IdMCabang = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGKBMRek MRek ON (TablePosRek.IdMCabang = MRek.IdMCabang AND TablePosRek.IdMRek = MRek.IdMRek)
                LEFT OUTER JOIN MGSYMUSerMRek MUserMRek ON (MUserMrek.IdMCabangMrek=Mrek.IdMCabang AND MUserMrek.IdMrek=Mrek.IdMrek)
            WHERE MCabang.Hapus = 0
            AND MCabang.Aktif = 1
            AND MRek.Hapus = 0
            AND MRek.Aktif = 1
            AND MCabang.KdMCabang LIKE '%SDM%'
            AND MCabang.NmMCabang LIKE '%SEJATI TEMBOK%'
            AND MRek.KdMRek LIKE '%%'
            AND MRek.NmMRek LIKE '%%'
            AND PosRek <> 0
            AND MUserMRek.IdMUser=1
            ORDER BY MCabang.KdMCabang, MRek.NmMRek`;
    }

    return sql;
}



exports.queryKartuBank = async (companyid, start, end, bank) => { 
  let qbank = "";
    if (bank != "") {
      qbank = "AND MRek.IDMBANK =" + bank;
  }
  var sql = "";
  if (companyid == companyWI) {
    sql = `SELECT MCabang.KdMCabang,
                MCabang.NmMCabang, 
                MRek.KdMRek, 
                MRek.NmMRek, 
                TableKartuRek.IdMRek, 
                TableKartuRek.IdMCabang, 
                Urut, 
                BuktiTrans, 
                NoRef, 
                CAST(TglTrans as DATE) as TglTrans, 
                TableKartuRek.Keterangan, Saldo, 
                JmlRek, 
                IF(Urut = 0, 0, IF(IF(IsNull(JmlRek), 0, JmlRek) > 0, IF(IsNull(JmlRek), 0, JmlRek), 0)) As Debit, 
                IF(Urut = 0, 0, IF(IF(IsNull(JmlRek), 0, JmlRek) >= 0, 0, IF(IsNull(JmlRek), 0, JmlRek))) As Kredit,
                MBank.NMMBANK,
                MRek.IDMBANK
            FROM (
                SELECT IdMCabang, IdMRek, 0 As Urut, 0 as JenisTrans, 0 as IdTrans, '-' As BuktiTrans, cast('${start} 00:00:00' as DateTime) As TglTrans, 0 As JmlRek, sum(JmlRek) As Saldo, 'Saldo Sebelumnya' As Keterangan
                , '-' As NoRef FROM (
                SELECT IdMCabang, IdMRek, 0 As JmlRek FROM MGKBMRek
                UNION ALL
                SELECT IdMCabang, IdMRek, JmlRek FROM MGKBLKartuBank where CAST(TglTrans as DATE) < CAST('${start} 00:00:00' AS DATE)
                ) TableSaldoAwal
                GROUP BY IdMCabang, IdMRek
                UNION ALL
                SELECT IdMCabang, IdMRek, 1 as Urut, JenisTrans, IdTrans, BuktiTrans, TglTrans, JmlRek, 0, Keterangan 
                , NoRef
                FROM MGKBLKartuBank
                WHERE CAST(TglTrans as DATE) >= CAST('${start} 00:00:00' AS DATE) and CAST(TglTrans as DATE) < CAST('${end} 00:00:00' AS DATE)
            ) TableKartuRek 
            LEFT OUTER JOIN MGSYMCabang MCabang ON (TableKartuRek.IdMCabang = MCabang.IdMCabang)
            LEFT OUTER JOIN MGKBMRek MRek ON (TableKartuRek.IdMCabang = MRek.IdMCabang AND TableKartuRek.IdMRek = MRek.IdMRek)
            LEFT OUTER JOIN MGKBMBANK MBank on MRek.IDMBANK = MBank.IDMBANK
            WHERE MCabang.Hapus = 0
            AND MRek.Hapus = 0 
            ${qbank}
            ORDER BY TableKartuRek.IdMCabang, TableKartuRek.IdMRek, Urut, TglTrans, JenisTrans, IdTrans`;
  }else{
    sql = `SELECT * FROM ( 
              SELECT  Bank.IdMCabang, Bank.IdTrans, Bank.JenisTrans AS JenisTransBank  
                ,IF(TBPiutD.JenisTrans = 'S', 'Saldo Awal'
                  , IF(TBPiutD.JenisTrans = 'T', 'Penjualan POS'
                  , IF(TBPiutD.JenisTrans = 'J', 'Penjualan'
                  , IF(TBPiutD.JenisTrans = 'R', 'Retur Penjualan'
                  , '')))) AS JenisTransStr
                ,IF(TBPiutD.JenisTrans = 'S', '-'
                  , IF(TBPiutD.JenisTrans = 'T'
                  , IF(COALESCE(TJualPOS.BuktiTJualPOS, '')='', TBPiutD.BuktiTrans, TJualPOS.BuktiTJualPOS)
                  , IF(TBPiutD.JenisTrans = 'J', IF(COALESCE(TJual.BuktiTJual,'')='', TBPiutD.BuktiTrans, TJual.BuktiTJual)
                  , IF(TBPiutD.JenisTrans = 'R', IF(COALESCE(TRJual.BuktiTRJual,'')='', TBPiutD.BuktiTrans, TRJual.BuktiTRJual)
                  , '')))) AS BuktiTransStr
                      , TBPiutD.JenisTrans, TBPiutD.JmLBayar
              FROM MGARTBPiutD TBPiutD 
                LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiutD.IdMCabang = TBPiut.IdMCabang AND TBPiutD.IdTBPiut = TBPiut.IdTBPiut)
                LEFT OUTER JOIN MGARTBPiutDB TPiutDB ON (TBPiut.IdMCabang = TPiutDB.IdMCabang AND TBPiut.IdTBPiut = TPiutDB.IdTBPiut)
                LEFT OUTER JOIN MGARTJualPOS TJualPOS ON (TBPiutD.IdMCabangTrans = TJualPOS.IdMCabang AND TBPiutD.IdTrans = TJualPOS.IdTJualPOS AND TBPiutD.JenisTrans = 'T')
                LEFT OUTER JOIN MGARTJual TJual ON (TBPiutD.IdMCabangTrans = TJual.IdMCabang AND TBPiutD.IdTrans = TJual.IdTJual AND TBPiutD.JenisTrans = 'J')
                LEFT OUTER JOIN MGARTRJual TRJual ON (TBPiutD.IdMCabangTrans = TRJual.IdMCabang AND TBPiutD.IdTrans = TRJual.IdTRJual AND TBPiutD.JenisTrans = 'R')
                LEFT OUTER JOIN MGKBLKartuBank Bank ON (TBPiutD.IdMCabang = Bank.IdMCabang AND TBPiutD.IdTBPiut =Bank.IdTRans )
              WHERE  Bank.JenisTrans = 8.1 AND TBPiut.Hapus=0 AND TBPiut.Void = 0
                    AND TPiutDB.JenisMRef = 'B'
          ) Piut

          UNION ALL
          
          SELECT * FROM (
              SELECT Bank.IdMCabang, Bank.IdTrans, Bank.JenisTrans AS JenisTransBank
                  , IF(TBHutD.JenisTrans = 'S', 'Saldo Awal'
                      , IF(TBHutD.JenisTrans = 'T', 'Pembelian'
                      , IF(TBHutD.JenisTrans = 'R', 'Retur Pembelian', ''))) AS JenisTransStr
                  , IF(TBHutD.JenisTrans = 'S', '-'
                      , IF(TBHutD.JenisTrans = 'T', IF(COALESCE(TBeli.BuktiTBeli,'')='', TBHutD.BuktiTrans, TBeli.BuktiTBeli)
                      , IF(TBHutD.JenisTrans = 'R', IF(COALESCE(TRBeli.BuktiTRBeli,'')='', TBHutD.BuktiTrans, TRBeli.BuktiTRBeli)
                      , ''))) AS BuktiTransStr
                  , TBHutD.JenisTrans, TBHutD.JmLBayar 
              FROM MGAPTBHutD TBHutD 
                LEFT OUTER JOIN MGAPTBHut TBHut  ON (TBHutD.IdMCabang = TBHut.IdMCabang AND TBHutD.IdTBHut = TBHut.IdTBHut)
                LEFT OUTER JOIN MGAPTBHutDB TBHutDB  ON (TBHut.IdMCabang = TBHutDB.IdMCabang AND TBHut.IdTBHut = TBHutDB.IdTBHut)
                LEFT OUTER JOIN MGAPTBeli TBeli ON (TBHutD.IdMCabangTrans = TBeli.IdMCabang AND TBHutD.IdTrans = TBeli.IdTBeli AND TBHutD.JenisTrans = 'T')
                LEFT OUTER JOIN MGAPTRBeli TRBeli ON (TBHutD.IdMCabangTrans = TRBeli.IdMCabang AND TBHutD.IdTrans = TRBeli.IdTRBeli AND TBHutD.JenisTrans = 'R')
                LEFT OUTER JOIN MGKBLKartuBank Bank ON (TBHutD.IdMCabang = Bank.IdMCabang AND TBHutD.IdTBHut =Bank.IdTRans ) 
              WHERE  Bank.JenisTrans = 6.1 AND TBHut.Hapus=0 AND TBHut.Void = 0 
                    AND TBHutDB.JenisMRef = 'B'
          ) Hut
        ORDER BY BuktiTransStr`
  }
  return sql;
}