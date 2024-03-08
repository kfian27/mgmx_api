exports.queryPosisiKasWI = async (date) => {
    var sql = `SELECT MCabang.KdMCabang, MCabang.NmMCabang, MCabang.Aktif, MKas.KdMKas, MKas.NmMKas, MKas.Aktif, TablePosKas.PosKas
        FROM (
            SELECT TransAll.IdMCabang, IdMKas, Sum(JmlKas) as PosKas FROM (
                Select k.TglTrans, k.IdMCabang, k.IdMKas, k.JmlKas FROM MGKBLKartuKas k
                    UNION ALL
                    SELECT '2024-03-31 00:00:00' as TglTrans, IdMCabang, IdMKas, 0 as JmlKas FROM MGKBMKas
                    ) TransAll
                    WHERE TglTrans < '${date} 00:00:00'
                    GROUP BY TransAll.IdMCabang, IdMKas
                ) TablePosKas LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosKas.IdMCabang = MCabang.IdMCabang)
        LEFT OUTER JOIN MGKBMKas MKas ON (TablePosKas.IdMCabang = MKas.IdMCabang AND TablePosKas.IdMKas = MKas.IdMKas)
        WHERE MCabang.Hapus = 0
            AND MCabang.Aktif = 1
            AND MKas.Hapus = 0
            AND MKas.Aktif = 1
            AND MKas.KdMKas LIKE '%%'
            AND MKas.NmMKas LIKE '%%'
            AND MKas.idmkas in (select idmkas from mgsymusermkas where idmuser=1 and idmcabangmuser=0 and idmcabangmkas=idmcabangmuser)
            AND PosKas <> 0
        ORDER BY MCabang.KdMCabang, MKas.NmMKas`;

    return sql;
}


exports.queryKartuKasWI = async (start, end, qkas) => {
    var sql = `SELECT TableKartuKas.IsDebetKredit 
                    , MCabang.KdMCabang
                    , MCabang.NmMCabang
                    , MKas.KdMKas
                    , MKas.NmMKas
                    , TableKartuKas.IdMKas
                    , TableKartuKas.IdMCabang
                    , Urut
                    , BuktiTrans
                    , TglTrans as TglUpdate
                    , CAST(TglTrans as DATE) as TglTrans
                    , TableKartuKas.Keterangan, TableKartuKas.noref, Saldo, JmlKas
                    , IF(Urut = 0, 0, IF(IF(IsNull(JmlKas), 0, JmlKas) > 0, IF(IsNull(JmlKas), 0, JmlKas), 0)) As Debit
                    , IF(Urut = 0, 0, IF(IF(IsNull(JmlKas), 0, JmlKas) >= 0, 0, IF(IsNull(JmlKas), 0, JmlKas))) As Kredit
                FROM (
                    SELECT IF(JmlKas >= 0, 1, 2) AS IsDebetKredit, IdMCabang,  IdMKas, 0 As Urut, 0 as JenisTrans, 0 as JenisRugiLaba, 0 as IdTrans, '-' As BuktiTrans, cast('${start} 00:00:00' as DateTime) As TglTrans, 0 As JmlKas, sum(JmlKas) As Saldo, concat('Saldo Awal per ',cast('${start} 00:00:00' as Date)) As Keterangan, '-' as noref 
                    FROM (
                        SELECT IdMCabang, IdMKas,  0 As JmlKas FROM MGKBMKas
                        UNION ALL
                        SELECT IdMCabang, IdMKas, JmlKas FROM MGKBLKartuKas where CAST(TglTrans as DATE) < CAST('${start} 00:00:00' AS DATE)
                    ) TableSaldoAwal
                    GROUP BY IdMCabang, IdMKas,noref
                UNION ALL
                    SELECT IF(JmlKas >= 0, 1, 2) AS IsDebetKredit, IdMCabang, IdMKas, 1 as Urut, JenisTrans, JenisRugiLaba, IdTrans, BuktiTrans, TglTrans, JmlKas, 0, Keterangan, noref
                    FROM MGKBLKartuKas where CAST(TglTrans as DATE) >= CAST('${start} 00:00:00' AS DATE) and CAST(TglTrans as DATE) < CAST('${end} 00:00:00' AS DATE)
                    ) TableKartuKas LEFT OUTER JOIN MGSYMCabang MCabang ON (TableKartuKas.IdMCabang = MCabang.IdMCabang)
                            LEFT OUTER JOIN MGKBMKas MKas ON (TableKartuKas.IdMCabang = MKas.IdMCabang AND TableKartuKas.IdMKas = MKas.IdMKas)
                WHERE MCabang.Hapus = 0
                    AND MCabang.Aktif = 1
                    AND MKas.Hapus = 0
                    AND MKas.Aktif = 1
                    AND MKas.KdMKas LIKE '%%'
                    AND MKas.NmMKas LIKE '%%'
                    AND MKas.idmkas in (select idmkas from mgsymusermkas where idmuser=1 and idmcabangmuser=0 and idmcabangmkas=idmcabangmuser)
                    ${qkas}
                ORDER BY TableKartuKas.IdMCabang, TableKartuKas.IdMKas, Urut, TglTrans, TglUpdate, JenisTrans, IdTrans, noref`;
    return sql;
}