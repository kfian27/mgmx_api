let today = new Date().toJSON().slice(0, 10);

exports.queryPosisiKasWI = async (date) => {
    return `SELECT MCabang.KdMCabang, MCabang.NmMCabang, MCabang.Aktif, MKas.KdMKas, MKas.NmMKas, MKas.Aktif, TablePosKas.PosKas
        FROM (
            SELECT TransAll.IdMCabang, IdMKas, Sum(JmlKas) as PosKas FROM (
                Select k.TglTrans, k.IdMCabang, k.IdMKas, k.JmlKas FROM MGKBLKartuKas k
                    UNION ALL
                    SELECT '2024-03-31 00:00:00' as TglTrans, IdMCabang, IdMKas, 0 as JmlKas FROM MGKBMKas
                    ) TransAll
                    WHERE TglTrans < ${date}
                    GROUP BY TransAll.IdMCabang, IdMKas
                ) TablePosKas LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosKas.IdMCabang = MCabang.IdMCabang)
        LEFT OUTER JOIN MGKBMKas MKas ON (TablePosKas.IdMCabang = MKas.IdMCabang AND TablePosKas.IdMKas = MKas.IdMKas)
        WHERE MCabang.Hapus = 0
            AND MCabang.Aktif = 1
            AND MKas.Hapus = 0
            AND MKas.Aktif = 1
            AND MCabang.KdMCabang LIKE '%AIH%'
            AND MCabang.NmMCabang LIKE '%ALAM INDAH HARMONI%'
            AND MKas.KdMKas LIKE '%%'
            AND MKas.NmMKas LIKE '%%'
            AND MKas.idmkas in (select idmkas from mgsymusermkas where idmuser=1 and idmcabangmuser=0 and idmcabangmkas=idmcabangmuser)
            AND PosKas <> 0
        ORDER BY MCabang.KdMCabang, MKas.NmMKas`;
}