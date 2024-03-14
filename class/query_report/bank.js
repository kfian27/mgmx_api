const fun = require("../../mgmx");
var companyWI = fun.companyWI;

exports.queryPosisiBank = async (companyid,tanggal) => {
    var sql = "";
    if (companyid == companyWI) {
      sql = `SELECT MCabang.KdMCabang,
              MCabang.NmMCabang, 
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
      sql = `SELECT MCabang.KdMCabang, MCabang.NmMCabang
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
  } else {
    sql = `SELECT MCabang.KdMCabang
        , MCabang.NmMCabang
        , MRek.KdMRek
        , MRek.NmMRek
        , MBank.NMMBANK
        , TableKartuRek.IdMRek
        , TableKartuRek.IdMCabang
        , Urut
        , BuktiTrans, NoRef
        , CAST(TglTrans as DATE) as TglTrans
        , TableKartuRek.Keterangan, Saldo, JmlRek
        , IF(Urut = 0, 0, IF(COALESCE(JmlRek,0) > 0,COALESCE(JmlRek,0), 0)) As Debit
        , IF(Urut = 0, 0, IF(COALESCE(JmlRek,0) >= 0, 0, COALESCE(JmlRek,0))) As Kredit
        , MCabang.IdMCabang, IdTrans, JenisTrans
    FROM (
    SELECT IdMCabang, IdMRek, 0 As Urut, 0 as JenisTrans, 0 as IdTrans, '-' As BuktiTrans, cast('${start} 00:00:00' as DateTime) As TglTrans, 0 As JmlRek, sum(JmlRek) As Saldo, 'Saldo Sebelumnya' As Keterangan
        , '-' As NoRef
    FROM (
    SELECT IdMCabang, IdMRek, 0 As JmlRek FROM MGKBMRek
    UNION ALL
    SELECT IdMCabang, IdMRek, JmlRek FROM MGKBLKartuBank where CAST(TglTrans as DATE) < CAST('${start} 00:00:00' AS DATE)
    ) TableSaldoAwal
    GROUP BY IdMCabang, IdMRek
    UNION ALL
    SELECT IdMCabang, IdMRek, 1 as Urut, JenisTrans, IdTrans, BuktiTrans, TglTrans, JmlRek, 0, Keterangan 
        , NoRef
    FROM MGKBLKartuBank
    WHERE CAST(TglTrans as DATE) >= CAST('${start} 00:00:00' AS DATE) and CAST(TglTrans as DATE) <= CAST('${end} 23:59:59' AS DATE)
    ) TableKartuRek 
    LEFT OUTER JOIN MGSYMCabang MCabang ON (TableKartuRek.IdMCabang = MCabang.IdMCabang)
    LEFT OUTER JOIN MGKBMRek MRek ON (TableKartuRek.IdMCabang = MRek.IdMCabang AND TableKartuRek.IdMRek = MRek.IdMRek)
    LEFT OUTER JOIN MGSYMUSerMRek MUserMRek ON (MUserMrek.IdMCabangMrek=Mrek.IdMCabang AND MUserMrek.IdMrek=Mrek.IdMrek)
    LEFT OUTER JOIN MGKBMBANK MBank on MRek.IDMBANK = MBank.IDMBANK 
    WHERE MCabang.Hapus = 0
    AND MCabang.KdMCabang LIKE '%%'
    AND MCabang.NmMCabang LIKE '%%'
    AND MRek.Hapus = 0
    AND MRek.KdMRek LIKE '%%'
    AND MRek.NmMRek LIKE '%%'
    AND MUserMRek.IdMUser=1
    ORDER BY TableKartuRek.IdMCabang, TableKartuRek.IdMRek, Urut, TglTrans, JenisTrans, IdTrans`;
   
  }
  return sql;
}