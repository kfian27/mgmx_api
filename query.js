exports.dashboard_totalpiutang = async (date = "") => {
    let sql = `select
        MCabang.idMCabang,
        MCabang.KdMCabang,
        MCabang.NmMCabang,
        MCabang.Aktif as AktifMCabang,
        coalesce(MCust.KdMCust, '') as KdMCust,
        coalesce(MCust.NmMCust, '') as NmMCust,
        coalesce(MCust.Aktif, 1) as AktifMCust,
        sum(coalesce(MCust.LimitPiut, 0)) as LimitPiut,
        sum(TablePosPiut.PosPiut) as total,
        sum(MCust.LimitPiut - TablePosPiut.PosPiut) as Selisih
    from
        (
        select
            TransAll.IdMCabang,
            TransAll.IdMCabangMCust,
            IdMCust,
            Sum(JmlPiut) as PosPiut
        from
            (
            select
                tbl.TglTrans,
                tbl.IdMCabang,
                tbl.IdMCabangMCust,
                tbl.IdMCust,
                tbl.JmlPiut,
                Tbl.id_bcf
            from
                (
                select
                    IdMCabang as IdMCabangMCust,
                    IdMCust,
                    0 as JenisTrans,
                    IdMCabang,
                    IdTSAPiut as IdTrans,
                    BuktiTSAPiut as BuktiTrans,
                    concat(Date(TglTSAPiut), ' ', Time(TglTSAPiut)) as TglTrans,
                    0 as JenisInvoice,
                    JmlPiut,
                    concat('Saldo Awal ', BuktiTSAPiut) as Keterangan,
                    -1 as Id_bcf
                from
                    MGARTSAPiut
                where
                    JmlPiut <> 0
                    and TglTSAPiut <= '${date}'
            union all
                select
                    IdMCabangMCust,
                    IdMCust,
                    1 as JenisTrans,
                    IdMCabang,
                    IdTJualPOS as IdTrans,
                    BuktiTJualPOS as BuktiTrans,
                    concat(Date(TglTJualPOS), ' ', Time(TglUpdate)) as TglTrans,
                    0 as JenisInvoice,
                    (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) as JmlPiut,
                    concat('Penjualan POS ', BuktiTJualPOS) as Keterangan,
                    -1 as Id_bcf
                from
                    MGARTJualPOS
                where
                    Hapus = 0
                    and Void = 0
                    and (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0
                    and TglTJualPOS <= '${date}'
            union all
                select
                    Jual.IdMCabangMCust,
                    Jual.IdMCust,
                    2 as JenisTrans,
                    Jual.IdMCabang,
                    Jual.IdTJual as IdTrans,
                    Jual.BuktiTJual as BuktiTrans,
                    concat(Date(Jual.TglTJual), ' ', Time(Jual.TglUpdate)) as TglTrans,
                    Jual.JenisTJual as JenisInvoice,
                    (Jual.JmlBayarKredit) as JmlPiut,
                    concat('Penjualan ', Jual.BuktiTJual) as Keterangan,
                    Jual.Id_bcf
                from
                    MGARTJual Jual
                where
                    Jual.Hapus = 0
                    and Jual.Void = 0
                    and (Jual.JmlBayarKredit) <> 0
                    and coalesce(Jual.IdTRJual, 0) = 0
                    and Jual.TglTJual <= '${date}'
            union all
                select
                    rj.IdMCabangMCust,
                    rj.IdMCust ,
                    3 as JenisTrans,
                    rj.IdMCabang,
                    rj.IdTRJual as IdTrans,
                    rj.BuktiTRJual as BuktiTrans,
                    concat(Date(rj.TglTRJual), ' ', Time(rj.TglUpdate)) as TglTrans,
                    rj.JenisInvoice as JenisInvoice,
                    - rj.JmlBayarKredit as JmlPiut,
                    concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan,
                    -1 as Id_bcf
                from
                    MGARTRJual rj
                left outer join MGARTJual jual on
                    (jual.IdMCabangTRJual = rj.IdMCabang
                        and jual.IdTRJual = rj.IdTRJual
                        and jual.Void = 0
                        and jual.Hapus = 0)
                left outer join MGARMCust MCust on
                    (MCust.IdMCabang = rj.IdMCabangMCust
                        and MCust.IdMCust = rj.IdMCust)
                where
                    rj.Hapus = 0
                    and rj.Void = 0
                    and rj.JmlBayarKredit <> 0
                    and rj.JenisRJual = 0
                    and jual.IdTJual is null
                    and TglTRJual <= '${date}'
            union all
                select
                    TJualLain.IdMCabangCust as IdMCabangMCust,
                    TJualLain.IdMCust as IdMCust,
                    2.1 as JenisTrans,
                    TJualLain.IdMCabang,
                    TJualLain.IdTJualLain as IdTrans,
                    TJualLain.BuktiTJualLain as BuktiTrans,
                    concat(Date(TJualLain.TglTJualLain), ' ', Time(TJualLain.TglUpdate)) as TglTrans,
                    0 as JenisInvoice,
                    TJualLain.Netto as JmlPiut,
                    CONCAT('Penjualan ', ' ', if(TJualLain.JenisEkspedisi = 1, 'FRANCO', if(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' ', if(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan,
                    -1 as Id_bcf
                from
                    MGARTJualLain TJualLain
                left outer join MGARMCust MCust on
                    (MCust.IdMCabang = TJualLain.IdMCabangCust
                        and MCust.IdMCust = TJualLain.IdMCust)
                where
                    TJualLain.Hapus = 0
                    and TJualLain.Void = 0
                    and TJualLain.Netto <> 0
                    and TglTJualLain <= '${date}'
            union all
                select
                    TAngkutan.IdMCabangMCust,
                    TAngkutan.IdMCust,
                    2.2 as JenisTrans,
                    TAngkutan.IdMCabang,
                    TAngkutan.IdTTAngkutan as IdTrans,
                    TAngkutan.BuktiTTAngkutan as BuktiTrans,
                    CONCAT(DATE(TAngkutan.TglTTAngkutan), ' ', TIME(TAngkutan.TglUpdate)) as TglTrans,
                    TAngkutan.JenisTTAngkutan as JenisInvoice,
                    (TAngkutan.JmlBayarKredit) as JmlPiut,
                    CONCAT('Titip Angkutan ', TAngkutan.BuktiTTAngkutan) as Keterangan,
                    -1 as Id_bcf
                from
                    MGARTTAngkutan TAngkutan
                where
                    Hapus = 0
                    and Void = 0
                    and (TAngkutan.JmlBayarKredit) <> 0
                    and TglTTAngkutan <= '${date}'
            union all
                select
                    m.IdMCabangMCust,
                    m.IdMCust,
                    4 as JenisTrans,
                    m.IdMCabang,
                    m.IdTBPiut as IdTrans,
                    m.BuktiTBPiut as BuktiTrans,
                    concat(Date(d.TglBayar), ' ', Time(m.TglUpdate)) as TglTrans,
                    m.JenisInvoice as JenisInvoice,
                    - (d.JmlBayar - coalesce(UMJUalD.JmlBayar, 0)) as JmlPiut,
                    concat('Pembayaran Piutang ', BuktiTBPiut, ' ' , if(d.jenisMref = 'K', Kas.KdMKas, if(d.JenisMRef = 'B', Rek.KdMRek, if(d.JenisMref = 'G' , Giro.KdMGiro, if(d.JenisMRef = 'P', Prk.KdMPrk, ''))))) as Keterangan,
                    m.Id_bcf
                from
                    MGARTBPiutDB d
                left outer join MGARTBPiut m on
                    (d.IdMCabang = m.IdMCabang
                        and d.IdTBPiut = m.IdTBPiut)
                left outer join MGARTUMJual UMJual on
                    (m.IdMCabang = UMJual.IdMCabangTBPiut
                        and m.IdTBPiut = UMJual.IdTBPiut
                        and UMJual.Hapus = 0
                        and UMJual.Void = 0)
                left outer join MGARTUMJualD UMJualD on
                    (UMJual.IdMCabang = UMJualD.IdMCabang
                        and UMJual.IdTUMJual = UMJualD.IdTUMJual
                        and d.IdMCabangMRef = UMJualD.IdMCabangMRef
                        and d.IdMref = UMJualD.IdMRef
                        and d.TglBayar = UMJualD.TglBayar)
                left outer join MGKBMKas Kas on
                    (Kas.IdMCabang = d.IdMCabangMref
                        and Kas.IdMKas = d.IdMref
                        and d.JenisMRef = 'K')
                left outer join MGKBMRek Rek on
                    (Rek.IdMCabang = d.IdMCabangMref
                        and Rek.IdMRek = d.IdMref
                        and d.JenisMRef = 'B')
                left outer join MGKBMGiro Giro on
                    (Giro.IdMCabang = d.IdMCabangMref
                        and Giro.IdMGiro = d.IdMref
                        and d.JenisMRef = 'G')
                left outer join MGGLMPrk Prk on
                    ( Prk.IdMPrk = d.IdMref
                        and d.JenisMRef = 'P'
                        and Prk.Periode = 0)
                where
                    m.Hapus = 0
                    and m.Void = 0
                    and m.Total <> 0
                    and d.TglBayar <= '${date}'
            union all
                select
                    m.IdMCabangMCust,
                    m.IdMCust,
                    4 as JenisTrans,
                    m.IdMCabang,
                    m.IdTBPiut as IdTrans,
                    m.BuktiTBPiut as BuktiTrans,
                    concat(Date(m.TglTBPiut), ' ', Time(m.TglUpdate)) as TglTrans,
                    m.JenisInvoice as JenisInvoice,
                    - m.JmlUM as JmlPiut,
                    concat('Pembayaran Piutang dgn Uang Muka ', BuktiTBPiut) as Keterangan,
                    m.Id_bcf
                from
                    MGARTBPiut m
                where
                    m.Hapus = 0
                    and m.Void = 0
                    and m.JmlUM <> 0
                    and TglTBPiut <= '${date}'
            union all
                select
                    MCust.IdMCabang,
                    MCust.IdMCust as IdMCust,
                    4.1 as JenisTrans,
                    TBPiut.IdMCabang,
                    TBPiut.IdTBPiut as IdTrans,
                    TBPiut.BuktiTBPiut as BuktiTrans,
                    CONCAT(DATE(TBPiutB.TglBayar), ' ', TIME(TBPiut.TglUpdate)) as TglTrans,
                    TBPiut.JenisInvoice as JenisInvoice,
                    TBPiutB.JMLBayar as JmlPiut,
                    CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') as Keterangan,
                    -1 as Id_bcf
                from
                    MGARTBPiutDB TBPiutB
                left outer join MGARTBPiut TBPiut on
                    (TBPiut.IdMCabang = TBPiutB.IdMCabang
                        and TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
                left outer join MGARMCust MCust on
                    (MCust.IdMCabang = TBPiut.IdMCabangMCust
                        and MCust.IdMCust = TBPiut.IdMCust)
                left outer join MGKBMGiro MG on
                    (MG.IdMCabang = TBPiutB.IdMCabangMREF
                        and MG.IdMGiro = TBPiutB.IdMREF)
                where
                    (TBPiut.HAPUS = 0
                        and TBPiut.VOID = 0)
                    and TBPiutB.JenisMREF = 'G'
                    and TBPiutB.TglBayar <= '${date}'
            union all
                select
                    MCust.IdMCabang as IdMCabang,
                    MCust.IdMCust as IdMCust,
                    4.2 as JenisTrans,
                    m.IdMCabang,
                    M.IdTGiroCair as IdTrans,
                    M.BuktiTGiroCair as BuktiTrans,
                    CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) as TglTrans,
                    BPiut.JenisInvoice as JenisInvoice,
                    -BPiutDB.JMLBayar as JmlPiut,
                    CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') as Keterangan,
                    -1 as Id_bcf
                from
                    MGKBTGiroCairD D
                left outer join MGKBTGiroCair M on
                    (M.IdMCabang = D.IdMCabang
                        and M.IdTGiroCair = D.IdTGiroCair)
                left outer join MGKBMGiro MG on
                    (MG.IdMCabang = D.IdMCabangMGiro
                        and MG.IdMGiro = D.IdMGiro)
                left outer join MGARMCust MCust on
                    (MCust.IdMCabang = MG.IdMCabangMCust
                        and MCust.IdMCust = MG.IdMCust
                        and MG.JenisMGiro = 'M')
                left outer join MGARTBPiutDB BPiutDB on
                    (BPiutDB.IdMCabangMRef = MG.IdMCabang
                        and BPiutDB.IdMRef = MG.IdMGiro
                        and BPiutDB.JenisMREF = 'G')
                left outer join MGARTBPiut BPiut on
                    (BPiut.IdMCabang = BPiutDB.IdMCabang
                        and BPiut.IdTBPiut = BPiutDB.IdTBPiut)
                where
                    (M.HAPUS = 0
                        and M.VOID = 0)
                    and M.JenisTGiroCair = 'M'
                    and BPiutDB.JenisMRef = 'G'
                    and BPiut.VOID = 0
                    and BPiut.HAPUS = 0
                    and TglTGiroCair <= '${date}'
            union all
                select
                    MCust.IdMCabang as IdMCabang,
                    MCust.IdMCust as IdMCust,
                    4.3 as JenisTrans,
                    m.IdMCabang,
                    M.IdTGiroTolak as IdTrans,
                    M.BuktiTGiroTolak as BuktiTrans,
                    CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) as TglTrans,
                    BPiut.JenisInvoice as JenisInvoice,
                    BPiutDB.JMLBayar as JmlPiut,
                    CONCAT('Giro Tolak ', m.BuktiTGiroTolak, ' (', MG.KdMGiro, ')') as Keterangan,
                    -1 as Id_bcf
                from
                    MGKBTGiroTolakD D
                left outer join MGKBTGiroTolak M on
                    (M.IdMCabang = D.IdMCabang
                        and M.IdTGiroTolak = D.IdTGiroTolak)
                left outer join MGKBMGiro MG on
                    (MG.IdMCabang = D.IdMCabangMGiro
                        and MG.IdMGiro = D.IdMGiro)
                left outer join MGARMCust MCust on
                    (MCust.IdMCabang = MG.IdMCabangMCust
                        and MCust.IdMCust = MG.IdMCust
                        and MG.JenisMGiro = 'M')
                left outer join MGARTBPiutDB BPiutDB on
                    (BPiutDB.IdMCabangMRef = MG.IdMCabang
                        and BPiutDB.IdMRef = MG.IdMGiro
                        and BPiutDB.JenisMREF = 'G')
                left outer join MGARTBPiut BPiut on
                    (BPiut.IdMCabang = BPiutDB.IdMCabang
                        and BPiut.IdTBPiut = BPiutDB.IdTBPiut)
                where
                    (M.HAPUS = 0
                        and M.VOID = 0)
                    and M.JenisTGiroTolak = 'M'
                    and BPiutDB.JenisMRef = 'G'
                    and BPiut.VOID = 0
                    and BPiut.HAPUS = 0
                    and TglTGiroTolak <= '${date}'
            union all
                select
                    MCust.IdMCabang as IdMCabang,
                    MCust.IdMCust as IdMCust,
                    4.4 as JenisTrans,
                    m.IdMCabang,
                    M.IDTGiroGanti as IdTrans,
                    M.BuktiTGiroGanti as BuktiTrans,
                    CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) as TglTrans,
                    BPiut.JenisInvoice as JenisInvoice,
                    -D.JMLBayar as JmlPiut,
                    CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') as Keterangan,
                    -1 as Id_bcf
                from
                    MGKBTGiroGanti M
                left outer join MGKBTGiroGantiDG D on
                    (M.IdMCabang = D.IdMCabang
                        and M.IDTGiroGanti = D.IDTGiroGanti)
                left outer join MGKBMGiro MG on
                    (MG.IdMCabang = D.IdMCabangMGiro
                        and MG.IdMGiro = D.IdMGiro)
                left outer join MGARMCust MCust on
                    (MCust.IdMCabang = MG.IdMCabangMCust
                        and MCust.IdMCust = MG.IdMCust
                        and MG.JenisMGiro = 'M')
                left outer join MGARTBPiutDB BPiutDB on
                    (BPiutDB.IdMCabangMRef = MG.IdMCabang
                        and BPiutDB.IdMRef = MG.IdMGiro
                        and BPiutDB.JenisMREF = 'G')
                left outer join MGARTBPiut BPiut on
                    (BPiut.IdMCabang = BPiutDB.IdMCabang
                        and BPiut.IdTBPiut = BPiutDB.IdTBPiut)
                where
                    (M.HAPUS = 0
                        and M.VOID = 0)
                    and M.JenisTGiroGanti = 'M'
                    and BPiutDB.JenisMRef = 'G'
                    and BPiut.VOID = 0
                    and BPiut.HAPUS = 0
                    and TglTGiroGanti >= '${date}'
                    and TglTGiroGanti < '1899-12-30'
            union all
                select
                    IdMCabangMCust,
                    IdMCust,
                    5 as JenisTrans,
                    IdMCabang,
                    IdTKorPiut as IdTrans,
                    BuktiTKorPiut as BuktiTrans,
                    concat(Date(TglTKorPiut), ' ', Time(TglUpdate)) as TglTrans,
                    IdMJenisInvoice as JenisInvoice,
                    Total as JmlPiut,
                    concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan,
                    -1 as Id_bcf
                from
                    MGARTKorPiut
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0
                    and TglTKorPiut <= '${date}'
            union all
                select
                    mj.IdMCabangMCust,
                    mj.IdMCust,
                    6 as JenisTrans,
                    m.IdMCabang,
                    m.IdTTagihan as IdTrans,
                    m.BuktiTTagihan as BuktiTrans,
                    CONCAT(DATE(TglTTagihan), ' ', TIME(m.TglUpdate)) as TglTrans,
                    0 as JenisInvoice,
                    -d.jmlbayar as JmlPiut,
                    CONCAT('Bayar Tagihan No. Jual : ', mj.buktiTJual, if(mg.KdMGiro <> '', CONCAT('(', mg.KdMGiro, ')'), '')) as Keterangan,
                    -1 as Id_bcf
                from
                    MGARTTagihanD d
                left outer join MGARTTagihan m on
                    (d.IdTTagihan = m.IdTTagihan)
                left outer join MGARTJual mj on
                    (d.IdTrans = mj.IdTJual)
                left outer join MGKBMGiro mg on
                    (d.IdMRef = mg.IdMGiro
                        and jenisMRef = 'G')
                where
                    m.Hapus = 0
                    and m.Void = 0
                    and d.jmlbayar <> 0
                    and TglTTagihan <= '${date}'
            union all
                select
                    mj.IdMCabang,
                    mj.IdMCust as IdMCust,
                    6.1 as JenisTrans,
                    m.IdMCabang,
                    m.IdTTagihan as IdTrans,
                    m.BuktiTTagihan as BuktiTrans,
                    CONCAT(DATE(m.TglTTagihan), ' ', TIME(m.TglUpdate)) as TglTrans,
                    0 as JenisInvoice,
                    sum(d.JMLBayar) as JmlPiut,
                    CONCAT('Titipan Giro ', m.buktiTTagihan, ' (', g.KdMGiro, ')' ) as Keterangan,
                    -1 as Id_bcf
                from
                    MGARTTagihanD D
                left outer join mgarttagihan m on
                    (d.IdTTagihan = m.IdTTagihan
                        and d.IdMCabang = m.IdMCabang)
                left outer join mgartjual mj on
                    (d.idMCabang = mj.IdMCabang
                        and d.IdTrans = mj.IdTJual)
                left outer join mgkbmgiro g on
                    (d.IdMRef = g.IdMGiro
                        and d.IdMCabang = g.IdMCabang)
                where
                    (m.HAPUS = 0
                        and m.VOID = 0)
                    and d.JenisMREF = 'G'
                    and TglTTagihan <= '${date}'
                group by
                    Keterangan
            union all
                select
                    MCust.IdMCabang as IdMCabang,
                    MCust.IdMCust as IdMCust,
                    6.2 as JenisTrans,
                    m.IdMCabang,
                    M.IdTGiroCair as IdTrans,
                    M.BuktiTGiroCair as BuktiTrans,
                    CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) as TglTrans,
                    0 as JenisInvoice,
                    -sum(mtd.JMLBayar) as JmlPiut,
                    CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') as Keterangan,
                    -1 as Id_bcf
                from
                    MGKBTGiroCairD D
                left outer join MGKBTGiroCair M on
                    (M.IdMCabang = D.IdMCabang
                        and M.IdTGiroCair = D.IdTGiroCair)
                left outer join MGKBMGiro MG on
                    (MG.IdMCabang = D.IdMCabangMGiro
                        and MG.IdMGiro = D.IdMGiro)
                left outer join MGARMCust MCust on
                    (MCust.IdMCabang = MG.IdMCabangMCust
                        and MCust.IdMCust = MG.IdMCust
                        and MG.JenisMGiro = 'M')
                left outer join MGARTTagihand mtd on
                    (mtd.IdMCabang = MG.IdMCabang
                        and mtd.IdMRef = MG.IdMGiro
                        and mtd.JenisMREF = 'G')
                left outer join mgarttagihan mt on
                    (mt.IdMCabang = mtd.IdMCabang
                        and mt.IdTTagihan = mtd.IdTTagihan)
                where
                    (M.HAPUS = 0
                        and M.VOID = 0)
                    and M.JenisTGiroCair = 'M'
                    and mtd.JenisMRef = 'G'
                    and mt.VOID = 0
                    and mt.HAPUS = 0
                    and TglTGiroCair <= '${date}'
                group by
                    IdMCust
            union all
                select
                    MCust.IdMCabang as IdMCabang,
                    MCust.IdMCust as IdMCust,
                    6.3 as JenisTrans,
                    m.IdMCabang,
                    M.IdTGiroTolak as IdTrans,
                    M.BuktiTGiroTolak as BuktiTrans,
                    CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) as TglTrans,
                    0 as JenisInvoice,
                    sum(mtd.JMLBayar) as JmlPiut,
                    CONCAT('Giro Tolak ', m.BuktiTGiroTolak, ' (', MG.KdMGiro, ')') as Keterangan,
                    -1 as Id_bcf
                from
                    MGKBTGiroTolakD D
                left outer join MGKBTGiroTolak M on
                    (M.IdMCabang = D.IdMCabang
                        and M.IdTGiroTolak = D.IdTGiroTolak)
                left outer join MGKBMGiro MG on
                    (MG.IdMCabang = D.IdMCabangMGiro
                        and MG.IdMGiro = D.IdMGiro)
                left outer join MGARMCust MCust on
                    (MCust.IdMCabang = MG.IdMCabangMCust
                        and MCust.IdMCust = MG.IdMCust
                        and MG.JenisMGiro = 'M')
                left outer join MGARTTagihand mtd on
                    (mtd.IdMCabang = MG.IdMCabang
                        and mtd.IdMRef = MG.IdMGiro
                        and mtd.JenisMREF = 'G')
                left outer join MGARTTagihan mt on
                    (mt.IdMCabang = mtd.IdMCabang
                        and mt.IdTTagihan = mtd.IdTTagihan)
                where
                    (M.HAPUS = 0
                        and M.VOID = 00)
                    and M.JenisTGiroTolak = 'M'
                    and mtd.JenisMRef = 'G'
                    and mt.VOID = 0
                    and mt.HAPUS = 0
                    and TglTGiroTolak <= '${date}'
                    group by IdMCust
            union all
                select
                    MCust.IdMCabang as IdMCabang,
                    MCust.IdMCust as IdMCust,
                    6.4 as JenisTrans,
                    m.IdMCabang,
                    M.IDTGiroGanti as IdTrans,
                    M.BuktiTGiroGanti as BuktiTrans,
                    CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) as TglTrans,
                    0 as JenisInvoice,
                    -D.JMLBayar as JmlPiut,
                    CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') as Keterangan,
                    -1 as Id_bcf
                from
                    MGKBTGiroGanti M
                left outer join MGKBTGiroGantiDG D on
                    (M.IdMCabang = D.IdMCabang
                        and M.IDTGiroGanti = D.IDTGiroGanti)
                left outer join MGKBMGiro MG on
                    (MG.IdMCabang = D.IdMCabangMGiro
                        and MG.IdMGiro = D.IdMGiro)
                left outer join MGARMCust MCust on
                    (MCust.IdMCabang = MG.IdMCabangMCust
                        and MCust.IdMCust = MG.IdMCust
                        and MG.JenisMGiro = 'M')
                left outer join MGARTTAgihanD TTagihD on
                    (TTagihD.IdMCabang = MG.IdMCabang
                        and TTagihd.IdMRef = MG.IdMGiro
                        and TTagihD.JenisMREF = 'G')
                left outer join MGARTTAgihan TTagih on
                    (TTagih.IdMCabang = TTagihD.IdMCabang
                        and TTagih.IdTTagihan = TTagihD.IdTTagihan)
                where
                    (M.HAPUS = 0
                        and M.VOID = 0)
                    and M.JenisTGiroGanti = 'M'
                    and TTagihD.JenisMRef = 'G'
                    and TTagih.VOID = 0
                    and TTagih.HAPUS = 0
                    and TglTGiroGanti <= '${date}') Tbl
            left outer join MGARMJenisInvoice JI on
                (Tbl.JenisInvoice = JI.IdMJenisInvoice
                    and Tbl.IdMCabang = JI.IdMCabang)
        union all
            select
                '${date}' as TglTrans,
                IdMCabang,
                IdMCabang as IdMCabangMCust,
                IdMCust,
                0 as JmlPiut,
                -1 as Id_bcf
            from
                MGARMCust) TransAll
        where
            TglTrans <= '${date}'
        group by
            TransAll.IdMCabang,
            TransAll.IdMCabangMCust,
            IdMCust) TablePosPiut
    left outer join MGSYMCabang MCabang on
        (TablePosPiut.IdMCabang = MCabang.IdMCabang)
    left outer join MGARMCust MCust on
        (TablePosPiut.IdMCabangMCust = MCust.IdMCabang
            and TablePosPiut.IdMCust = MCust.IdMCust)
    where
        MCabang.Hapus = 0
        and MCust.Hapus = 0
    order by
        MCabang.KdMCabang,
        MCust.NmMCust`;
    return sql;
}

exports.dashboard_totalpiutang = async (date = "") => { 
    let sql = `select
        MSup.KdMSup,
        MSup.NmMSup,
        MSup.Aktif,
        sum(TablePosHut.PosHut) as total
    from
        (
        select
            IdMSup,
            SUM(JmlHut) as PosHut
        from
            (
            select
                TglTrans,
                IdMSup,
                JmlHut
            from
                (
                select
                    IdMSup ,
                    0 as JenisTrans ,
                    IdTSAHut as IdTrans ,
                    BuktiTSAHut as BuktiTrans ,
                    CONCAT(DATE(TglTSAHut), ' ', TIME(TglUpdate)) as TglTrans ,
                    JmlHut ,
                    'Saldo Awal' as Keterangan
                from
                    MGAPTSAHut
                where
                    JmlHut <> 0
            union all
                select
                    IdMSup,
                    1 as JenisTrans,
                    IdTBeli as IdTrans,
                    if(BuktiTBeli = '',
                    BuktiTLPB,
                    BuktiTBeli) as BuktiTrans,
                    CONCAT(DATE(TglTBeli), ' ', TIME(TglUpdate)) as TglTrans,
                    (Netto - JmlBayarTunai) as JmlHut ,
                    CONCAT('Pembelian ', BuktiTBeli) as Keterangan
                from
                    MGAPTBeli
                where
                    Hapus = 0
                    and Void = 0
                    and (Netto - JmlBayarTunai) <> 0
                    and HapusLPB = 0
                    and VoidLPB = 0
                    and BuktiTBeli <> ''
            union all
                select
                    IdMSup ,
                    1.1 as JenisTrans ,
                    IdTBeliLain as IdTrans ,
                    BuktiTBeliLain as BuktiTrans ,
                    CONCAT(DATE(TglTBeliLain), ' ', TIME(TglUpdate)) as TglTrans ,
                    (JmlBayarKredit) as JmlHut ,
                    CONCAT('Biaya Lain-Lain ', BuktiTBeliLain) as Keterangan
                from
                    MGAPTBeliLain
                where
                    Hapus = 0
                    and Void = 0
                    and (JmlBayarKredit) <> 0
            union all
                select
                    IdMSup ,
                    2 as JenisTrans ,
                    IdTRBeli as IdTrans ,
                    BuktiTRBeli as BuktiTrans ,
                    CONCAT(DATE(TglTRBeli), ' ', TIME(TglUpdate)) as TglTrans ,
                    - (Netto - JmlBayarTunai) as JmlHut ,
                    CONCAT('Retur Pembelian ', BuktiTRBeli) as Keterangan
                from
                    MGAPTRBeli
                where
                    Hapus = 0
                    and Void = 0
                    and JenisTRBeli = 0
                    and (Netto - JmlBayarTunai) <> 0
            union all
                select
                    IdMSup,
                    3 as JenisTrans,
                    IdTPBeli as IdTrans,
                    BuktiTPBeli as BuktiTrans,
                    CONCAT(DATE(TglTPBeli), ' ', TIME(TglUpdate)) as TglTrans,
                    - Total as JmlHut,
                    CONCAT('Potongan Pembelian ', BuktiTPBeli) as Keterangan
                from
                    MGAPTPBeli
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0
            union all
                select
                    IdMSup,
                    4 as JenisTrans,
                    IdTBHut as IdTrans,
                    BuktiTBHut as BuktiTrans,
                    CONCAT(DATE(TglTBHut), ' ', TIME(TglUpdate)) as TglTrans,
                    - Total as JmlHut,
                    CONCAT('Pembayaran Hutang ', BuktiTBHut) as Keterangan
                from
                    MGAPTBHut
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0
            union all
                select
                    hut.IdMSup,
                    4.1 as JenisTrans,
                    Hut.IdTBHut as IdTrans,
                    BuktiTBHut as BuktiTrans,
                    CONCAT(DATE(Hut.TglTBHut), ' ', TIME(Hut.TglUpdate)) as tgltrans,
                    HutDB.JmlBayar as JmlHut,
                    CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')') as Keterangan
                from
                    MGAPTBHUTDB HutDB
                left outer join MGAPTBHut Hut on
                    (hut.idmcabang = hutDB.idmcabang
                        and hut.idtbhut = hutdb.idtbhut)
                left outer join mgapmsup sup on
                    (sup.idmsup = hut.idmsup)
                left outer join MGKBMGiro giro on
                    (giro.idmcabang = HutDB.IdMCabangMRef
                        and giro.IdMGiro = HutDB.IdMRef)
                where
                    (Hut.Hapus = 0
                        and Hut.Void = 0)
                    and HutDB.JenisMRef = 'G'
            union all
                select
                    sup.idmsup,
                    4.2 as JenisTrans,
                    m.idtgirocair as IdTrans,
                    m.BuktiTGiroCair as BuktiTrans,
                    CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) as TglTrans,
                    - HutDB.JmlBayar as JmlHut,
                    CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')') as Keterangan
                from
                    MGKBTGiroCairD d
                left outer join MGKBTGiroCair m on
                    (m.IdMCabang = d.IdMCabang
                        and d.idtgirocair = m.idtgirocair)
                left outer join MGKBMGiro giro on
                    (giro.idmcabang = d.idmcabangmgiro
                        and giro.idmgiro = d.idmgiro)
                left outer join MGAPMSup sup on
                    (sup.idmsup = giro.idmsup
                        and giro.jenismgiro = 'K')
                left outer join MGAPTBHutDB HutDB on
                    (HutDB.IdMCabangMRef = giro.IdMCabang
                        and HutDB.IdMRef = giro.IdMGiro
                        and HutDB.JenisMRef = 'G')
                left outer join MGAPTBHut Hut on
                    (Hut.IdMCabang = HutDB.IdMCabang
                        and hut.idtbhut = hutDB.idtbhut)
                where
                    (m.Hapus = 0
                        and m.Void = 0)
                    and m.JenisTGiroCair = 'K'
                    and HUTDB.JenisMRef = 'G'
                    and HUT.Void = 0
                    and HUT.Hapus = 0
            union all
                select
                    Sup.idmsup,
                    4.3 as JenisTrans,
                    m.idtgirotolak as IdTrans,
                    m.buktitgirotolak as BuktiTrans,
                    CONCAT(DATE(m.tgltgirotolak), ' ', TIME(m.tglupdate)) as TglTrans,
                    HutDB.jmlbayar as JmlHut,
                    CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')') as keterangan
                from
                    MGKBTGiroTolakD d
                left outer join MGKBTGiroTolak m on
                    (m.IdMCabang = d.IdMCabang
                        and m.IdTGiroTolak = d.IdTGiroTolak)
                left outer join MGKBMGiro giro on
                    (giro.IdMCabang = d.IdMCabangMGiro
                        and giro.IdMGiro = d.IdMGiro)
                left outer join MGAPMSup sup on
                    (sup.IdMSup = giro.IdMSup
                        and giro.JenisMGiro = 'K')
                left outer join MGAPTBHutDB HutDB on
                    (HutDB.IdMCabangMRef = giro.IdMCabang
                        and HutDB.IdMRef = giro.IdMGiro
                        and HutDB.JenisMRef = 'G')
                left outer join MGAPTBHut Hut on
                    (Hut.IdMCabang = HutDB.IdMCabang
                        and Hut.IdTBHut = HutDB.IdTBHut)
                where
                    (m.Hapus = 0
                        and m.Void = 0)
                    and m.JenisTGiroTolak = 'K'
                    and HutDB.JenisMRef = 'G'
                    and Hut.Void = 0
                    and Hut.Hapus = 0
            union all
                select
                    Sup.IdMSup as IdMSup,
                    4.4 as JenisTrans,
                    m.IdTGiroGanti as IdTrans,
                    m.BuktiTGiroGanti as BuktiTrans,
                    CONCAT(DATE(m.tgltgiroganti), ' ', TIME(m.tglupdate)) as tgltrans,
                    -d.JmlBayar as JmlHut,
                    CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')') as Keterangan
                from
                    MGKBTGiroGantiDG d
                left outer join MGKBTGiroGanti m on
                    (m.IdMCabang = d.IdMCabang
                        and m.IDTGiroGanti = d.IDTGiroGanti)
                left outer join MGKBMGiro giro on
                    (giro.IdMCabang = d.IdMCabangMGiro
                        and giro.IdMGiro = d.IdMGiro)
                left outer join MGAPMSup Sup on
                    (Sup.IdMSup = giro.IdMSup
                        and Giro.JenisMGiro = 'K')
                left outer join MGAPTBHutDB HutDB on
                    (HutDB.IdMCabangMRef = giro.IdMCabang
                        and HutDB.IdMRef = Giro.IdMGiro
                        and HutDB.JenisMREF = 'G')
                left outer join MGAPTBHut Hut on
                    (Hut.IdMCabang = HutDB.IdMCabang
                        and Hut.IdTBHut = HutDB.IdTBHut)
                where
                    (m.Hapus = 0
                        and m.Void = 0)
                    and m.JenisTGiroGanti = 'K'
                    and HutDB.JenisMRef = 'G'
                    and Hut.Void = 0
                    and Hut.Hapus = 0
            union all
                select
                    IdMSup,
                    5 as JenisTrans,
                    IdTKorHut as IdTrans,
                    BuktiTKorHut as BuktiTrans,
                    CONCAT(DATE(TglTKorHut), ' ', TIME(TglUpdate)) as TglTrans,
                    Total as JmlHut,
                    CONCAT('Koreksi Hutang ', BuktiTKorHut) as Keterangan
                from
                    MGAPTKorHut
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0) Tbl
        union all
            select
                '${date}' as TglTrans,
                IdMSup,
                0 as PosHut
            from
                MGAPMSup) TransAll
        where
            TglTrans <= '${date}'
        group by
            IdMSup) TablePosHut
    left outer join MGAPMSup MSup on
        (TablePosHut.IdMSup = MSup.IdMSup)
    where
        MSup.Hapus = 0
        and MSup.Aktif = 1`;

    return sql;
}

exports.dashboard_grafikjual = async (tgl2 = "") => { 
    var sqldiscount =`
        (SUM( 
            ( 
                SELECT SUM(
                    IF (( 
                        SELECT komisi 
                        FROM   mginmbrgdfoodcourt 
                        WHERE  idmbrg = tjuald.idmbrg 
                        AND    tglberlaku = 
                            ( 
                                SELECT MAX(tglberlaku) 
                                FROM   mginmbrgdfoodcourt 
                                WHERE  idmbrg = tjuald.idmbrg 
                                AND    tglberlaku <= tjual.tgltjual)
                        )<>0,
                                    
                    (tjuald.subtotal - (100/(100+ 
                        ( 
                        SELECT komisi 
                        FROM   mginmbrgdfoodcourt 
                        WHERE  idmbrg = tjuald.idmbrg 
                        AND    tglberlaku = 
                            ( 
                                SELECT MAX(tglberlaku) 
                                FROM   mginmbrgdfoodcourt 
                                WHERE  idmbrg = tjuald.idmbrg 
                                AND    tglberlaku <= tjual.tgltjual)
                        ) 
                            ) * tjuald.subtotal)
                    ), tjuald.subtotal)
                    ) 
                FROM   mgartjuald tjuald 
                WHERE  idtjual=tjual.idtjual
            )
        )
    )`;
    var sql = `select ${sqldiscount} as jumlah from mgartjual TJual where tgltjual = '${tgl2}%' and hapus=0`;
    return sql;
}

exports.dashboard_grafikhutang = async (tgl2 = "") => { 
    let sql = `select
        MSup.KdMSup,
        MSup.NmMSup,
        MSup.Aktif,
        sum(TablePosHut.PosHut) as jumlah
    from
        (
        select
            IdMSup,
            SUM(JmlHut) as PosHut
        from
            (
            select
                TglTrans,
                IdMSup,
                JmlHut
            from
                (
                select
                    IdMSup ,
                    0 as JenisTrans ,
                    IdTSAHut as IdTrans ,
                    BuktiTSAHut as BuktiTrans ,
                    CONCAT(DATE(TglTSAHut), ' ', TIME(TglUpdate)) as TglTrans ,
                    JmlHut ,
                    'Saldo Awal' as Keterangan
                from
                    MGAPTSAHut
                where
                    JmlHut <> 0
            union all
                select
                    IdMSup ,
                    1 as JenisTrans ,
                    IdTBeli as IdTrans ,
                    if(BuktiTBeli = '',
                    BuktiTLPB,
                    BuktiTBeli) as BuktiTrans ,
                    CONCAT(DATE(TglTBeli), ' ', TIME(TglUpdate)) as TglTrans ,
                    (Netto - JmlBayarTunai) as JmlHut ,
                    CONCAT('Pembelian ', BuktiTBeli) as Keterangan
                from
                    MGAPTBeli
                where
                    Hapus = 0
                    and Void = 0
                    and (Netto - JmlBayarTunai) <> 0
                    and HapusLPB = 0
                    and VoidLPB = 0
                    and BuktiTBeli <> ''
            union all
                select
                    IdMSup ,
                    1.1 as JenisTrans ,
                    IdTBeliLain as IdTrans ,
                    BuktiTBeliLain as BuktiTrans ,
                    CONCAT(DATE(TglTBeliLain), ' ', TIME(TglUpdate)) as TglTrans ,
                    (JmlBayarKredit) as JmlHut ,
                    CONCAT('Biaya Lain-Lain ', BuktiTBeliLain) as Keterangan
                from
                    MGAPTBeliLain
                where
                    Hapus = 0
                    and Void = 0
                    and (JmlBayarKredit) <> 0
            union all
                select
                    IdMSup ,
                    2 as JenisTrans ,
                    IdTRBeli as IdTrans ,
                    BuktiTRBeli as BuktiTrans ,
                    CONCAT(DATE(TglTRBeli), ' ', TIME(TglUpdate)) as TglTrans ,
                    - (Netto - JmlBayarTunai) as JmlHut ,
                    CONCAT('Retur Pembelian ', BuktiTRBeli) as Keterangan
                from
                    MGAPTRBeli
                where
                    Hapus = 0
                    and Void = 0
                    and JenisTRBeli = 0
                    and (Netto - JmlBayarTunai) <> 0
            union all
                select
                    IdMSup ,
                    3 as JenisTrans ,
                    IdTPBeli as IdTrans ,
                    BuktiTPBeli as BuktiTrans ,
                    CONCAT(DATE(TglTPBeli), ' ', TIME(TglUpdate)) as TglTrans ,
                    - Total as JmlHut ,
                    CONCAT('Potongan Pembelian ', BuktiTPBeli) as Keterangan
                from
                    MGAPTPBeli
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0
            union all
                select
                    IdMSup ,
                    4 as JenisTrans ,
                    IdTBHut as IdTrans ,
                    BuktiTBHut as BuktiTrans ,
                    CONCAT(DATE(TglTBHut), ' ', TIME(TglUpdate)) as TglTrans ,
                    - Total as JmlHut ,
                    CONCAT('Pembayaran Hutang ', BuktiTBHut) as Keterangan
                from
                    MGAPTBHut
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0
            union all
                select
                    hut.IdMSup ,
                    4.1 as JenisTrans ,
                    Hut.IdTBHut as IdTrans ,
                    BuktiTBHut as BuktiTrans ,
                    CONCAT(DATE(Hut.TglTBHut), ' ', TIME(Hut.TglUpdate)) as tgltrans ,
                    HutDB.JmlBayar as JmlHut ,
                    CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')') as Keterangan
                from
                    MGAPTBHUTDB HutDB
                left outer join MGAPTBHut Hut on
                    (hut.idmcabang = hutDB.idmcabang
                        and hut.idtbhut = hutdb.idtbhut)
                left outer join mgapmsup sup on
                    (sup.idmsup = hut.idmsup)
                left outer join MGKBMGiro giro on
                    (giro.idmcabang = HutDB.IdMCabangMRef
                        and giro.IdMGiro = HutDB.IdMRef)
                where
                    (Hut.Hapus = 0
                        and Hut.Void = 0)
                    and HutDB.JenisMRef = 'G'
            union all
                select
                    sup.idmsup ,
                    4.2 as JenisTrans ,
                    m.idtgirocair as IdTrans ,
                    m.BuktiTGiroCair as BuktiTrans ,
                    CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) as TglTrans ,
                    - HutDB.JmlBayar as JmlHut ,
                    CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')') as Keterangan
                from
                    MGKBTGiroCairD d
                left outer join MGKBTGiroCair m on
                    (m.IdMCabang = d.IdMCabang
                        and d.idtgirocair = m.idtgirocair)
                left outer join MGKBMGiro giro on
                    (giro.idmcabang = d.idmcabangmgiro
                        and giro.idmgiro = d.idmgiro)
                left outer join MGAPMSup sup on
                    (sup.idmsup = giro.idmsup
                        and giro.jenismgiro = 'K')
                left outer join MGAPTBHutDB HutDB on
                    (HutDB.IdMCabangMRef = giro.IdMCabang
                        and HutDB.IdMRef = giro.IdMGiro
                        and HutDB.JenisMRef = 'G')
                left outer join MGAPTBHut Hut on
                    (Hut.IdMCabang = HutDB.IdMCabang
                        and hut.idtbhut = hutDB.idtbhut)
                where
                    (m.Hapus = 0
                        and m.Void = 0)
                    and m.JenisTGiroCair = 'K'
                    and HUTDB.JenisMRef = 'G'
                    and HUT.Void = 0
                    and HUT.Hapus = 0
            union all
                select
                    Sup.idmsup ,
                    4.3 as JenisTrans ,
                    m.idtgirotolak as IdTrans ,
                    m.buktitgirotolak as BuktiTrans ,
                    CONCAT(DATE(m.tgltgirotolak), ' ', TIME(m.tglupdate)) as TglTrans ,
                    HutDB.jmlbayar as JmlHut ,
                    CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')') as keterangan
                from
                    MGKBTGiroTolakD d
                left outer join MGKBTGiroTolak m on
                    (m.IdMCabang = d.IdMCabang
                        and m.IdTGiroTolak = d.IdTGiroTolak)
                left outer join MGKBMGiro giro on
                    (giro.IdMCabang = d.IdMCabangMGiro
                        and giro.IdMGiro = d.IdMGiro)
                left outer join MGAPMSup sup on
                    (sup.IdMSup = giro.IdMSup
                        and giro.JenisMGiro = 'K')
                left outer join MGAPTBHutDB HutDB on
                    (HutDB.IdMCabangMRef = giro.IdMCabang
                        and HutDB.IdMRef = giro.IdMGiro
                        and HutDB.JenisMRef = 'G')
                left outer join MGAPTBHut Hut on
                    (Hut.IdMCabang = HutDB.IdMCabang
                        and Hut.IdTBHut = HutDB.IdTBHut)
                where
                    (m.Hapus = 0
                        and m.Void = 0)
                    and m.JenisTGiroTolak = 'K'
                    and HutDB.JenisMRef = 'G'
                    and Hut.Void = 0
                    and Hut.Hapus = 0
            union all
                select
                    Sup.IdMSup as IdMSup ,
                    4.4 as JenisTrans ,
                    m.IdTGiroGanti as IdTrans ,
                    m.BuktiTGiroGanti as BuktiTrans ,
                    CONCAT(DATE(m.tgltgiroganti), ' ', TIME(m.tglupdate)) as tgltrans ,
                    -d.JmlBayar as JmlHut ,
                    CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')') as Keterangan
                from
                    MGKBTGiroGantiDG d
                left outer join MGKBTGiroGanti m on
                    (m.IdMCabang = d.IdMCabang
                        and m.IDTGiroGanti = d.IDTGiroGanti)
                left outer join MGKBMGiro giro on
                    (giro.IdMCabang = d.IdMCabangMGiro
                        and giro.IdMGiro = d.IdMGiro)
                left outer join MGAPMSup Sup on
                    (Sup.IdMSup = giro.IdMSup
                        and Giro.JenisMGiro = 'K')
                left outer join MGAPTBHutDB HutDB on
                    (HutDB.IdMCabangMRef = giro.IdMCabang
                        and HutDB.IdMRef = Giro.IdMGiro
                        and HutDB.JenisMREF = 'G')
                left outer join MGAPTBHut Hut on
                    (Hut.IdMCabang = HutDB.IdMCabang
                        and Hut.IdTBHut = HutDB.IdTBHut)
                where
                    (m.Hapus = 0
                        and m.Void = 0)
                    and m.JenisTGiroGanti = 'K'
                    and HutDB.JenisMRef = 'G'
                    and Hut.Void = 0
                    and Hut.Hapus = 0
            union all
                select
                    IdMSup ,
                    5 as JenisTrans ,
                    IdTKorHut as IdTrans ,
                    BuktiTKorHut as BuktiTrans ,
                    CONCAT(DATE(TglTKorHut), ' ', TIME(TglUpdate)) as TglTrans ,
                    Total as JmlHut ,
                    CONCAT('Koreksi Hutang ', BuktiTKorHut) as Keterangan
                from
                    MGAPTKorHut
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0) Tbl
        union all
            select
                '${tgl2}' as TglTrans,
                IdMSup,
                0 as PosHut
            from
                MGAPMSup ) TransAll
        where
            TglTrans = '${tgl2}'
        group by
            IdMSup ) TablePosHut
    left outer join MGAPMSup MSup on
        (TablePosHut.IdMSup = MSup.IdMSup)
    where
        MSup.Hapus = 0
        and MSup.Aktif = 1`;
    
    return sql;
}

exports.dashboard_grafikpiutang = async (tgl2 = "") => { 
    let sql = `select
        MSup.KdMSup,
        MSup.NmMSup,
        MSup.Aktif,
        sum(TablePosHut.PosHut) as jumlah
    from
        (
        select
            IdMSup,
            SUM(JmlHut) as PosHut
        from
            (
            select
                TglTrans,
                IdMSup,
                JmlHut
            from
                (
                select
                    IdMSup ,
                    0 as JenisTrans ,
                    IdTSAHut as IdTrans ,
                    BuktiTSAHut as BuktiTrans ,
                    CONCAT(DATE(TglTSAHut), ' ', TIME(TglUpdate)) as TglTrans ,
                    JmlHut ,
                    'Saldo Awal' as Keterangan
                from
                    MGAPTSAHut
                where
                    JmlHut <> 0
            union all
                select
                    IdMSup ,
                    1 as JenisTrans ,
                    IdTBeli as IdTrans ,
                    if(BuktiTBeli = '',
                    BuktiTLPB,
                    BuktiTBeli) as BuktiTrans ,
                    CONCAT(DATE(TglTBeli), ' ', TIME(TglUpdate)) as TglTrans ,
                    (Netto - JmlBayarTunai) as JmlHut ,
                    CONCAT('Pembelian ', BuktiTBeli) as Keterangan
                from
                    MGAPTBeli
                where
                    Hapus = 0
                    and Void = 0
                    and (Netto - JmlBayarTunai) <> 0
                    and HapusLPB = 0
                    and VoidLPB = 0
                    and BuktiTBeli <> ''
            union all
                select
                    IdMSup ,
                    1.1 as JenisTrans ,
                    IdTBeliLain as IdTrans ,
                    BuktiTBeliLain as BuktiTrans ,
                    CONCAT(DATE(TglTBeliLain), ' ', TIME(TglUpdate)) as TglTrans ,
                    (JmlBayarKredit) as JmlHut ,
                    CONCAT('Biaya Lain-Lain ', BuktiTBeliLain) as Keterangan
                from
                    MGAPTBeliLain
                where
                    Hapus = 0
                    and Void = 0
                    and (JmlBayarKredit) <> 0
            union all
                select
                    IdMSup ,
                    2 as JenisTrans ,
                    IdTRBeli as IdTrans ,
                    BuktiTRBeli as BuktiTrans ,
                    CONCAT(DATE(TglTRBeli), ' ', TIME(TglUpdate)) as TglTrans ,
                    - (Netto - JmlBayarTunai) as JmlHut ,
                    CONCAT('Retur Pembelian ', BuktiTRBeli) as Keterangan
                from
                    MGAPTRBeli
                where
                    Hapus = 0
                    and Void = 0
                    and JenisTRBeli = 0
                    and (Netto - JmlBayarTunai) <> 0
            union all
                select
                    IdMSup ,
                    3 as JenisTrans ,
                    IdTPBeli as IdTrans ,
                    BuktiTPBeli as BuktiTrans ,
                    CONCAT(DATE(TglTPBeli), ' ', TIME(TglUpdate)) as TglTrans ,
                    - Total as JmlHut ,
                    CONCAT('Potongan Pembelian ', BuktiTPBeli) as Keterangan
                from
                    MGAPTPBeli
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0
            union all
                select
                    IdMSup ,
                    4 as JenisTrans ,
                    IdTBHut as IdTrans ,
                    BuktiTBHut as BuktiTrans ,
                    CONCAT(DATE(TglTBHut), ' ', TIME(TglUpdate)) as TglTrans ,
                    - Total as JmlHut ,
                    CONCAT('Pembayaran Hutang ', BuktiTBHut) as Keterangan
                from
                    MGAPTBHut
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0
            union all
                select
                    hut.IdMSup ,
                    4.1 as JenisTrans ,
                    Hut.IdTBHut as IdTrans ,
                    BuktiTBHut as BuktiTrans ,
                    CONCAT(DATE(Hut.TglTBHut), ' ', TIME(Hut.TglUpdate)) as tgltrans ,
                    HutDB.JmlBayar as JmlHut ,
                    CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')') as Keterangan
                from
                    MGAPTBHUTDB HutDB
                left outer join MGAPTBHut Hut on
                    (hut.idmcabang = hutDB.idmcabang
                        and hut.idtbhut = hutdb.idtbhut)
                left outer join mgapmsup sup on
                    (sup.idmsup = hut.idmsup)
                left outer join MGKBMGiro giro on
                    (giro.idmcabang = HutDB.IdMCabangMRef
                        and giro.IdMGiro = HutDB.IdMRef)
                where
                    (Hut.Hapus = 0
                        and Hut.Void = 0)
                    and HutDB.JenisMRef = 'G'
            union all
                select
                    sup.idmsup ,
                    4.2 as JenisTrans ,
                    m.idtgirocair as IdTrans ,
                    m.BuktiTGiroCair as BuktiTrans ,
                    CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) as TglTrans ,
                    - HutDB.JmlBayar as JmlHut ,
                    CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')') as Keterangan
                from
                    MGKBTGiroCairD d
                left outer join MGKBTGiroCair m on
                    (m.IdMCabang = d.IdMCabang
                        and d.idtgirocair = m.idtgirocair)
                left outer join MGKBMGiro giro on
                    (giro.idmcabang = d.idmcabangmgiro
                        and giro.idmgiro = d.idmgiro)
                left outer join MGAPMSup sup on
                    (sup.idmsup = giro.idmsup
                        and giro.jenismgiro = 'K')
                left outer join MGAPTBHutDB HutDB on
                    (HutDB.IdMCabangMRef = giro.IdMCabang
                        and HutDB.IdMRef = giro.IdMGiro
                        and HutDB.JenisMRef = 'G')
                left outer join MGAPTBHut Hut on
                    (Hut.IdMCabang = HutDB.IdMCabang
                        and hut.idtbhut = hutDB.idtbhut)
                where
                    (m.Hapus = 0
                        and m.Void = 0)
                    and m.JenisTGiroCair = 'K'
                    and HUTDB.JenisMRef = 'G'
                    and HUT.Void = 0
                    and HUT.Hapus = 0
            union all
                select
                    Sup.idmsup ,
                    4.3 as JenisTrans ,
                    m.idtgirotolak as IdTrans ,
                    m.buktitgirotolak as BuktiTrans ,
                    CONCAT(DATE(m.tgltgirotolak), ' ', TIME(m.tglupdate)) as TglTrans ,
                    HutDB.jmlbayar as JmlHut ,
                    CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')') as keterangan
                from
                    MGKBTGiroTolakD d
                left outer join MGKBTGiroTolak m on
                    (m.IdMCabang = d.IdMCabang
                        and m.IdTGiroTolak = d.IdTGiroTolak)
                left outer join MGKBMGiro giro on
                    (giro.IdMCabang = d.IdMCabangMGiro
                        and giro.IdMGiro = d.IdMGiro)
                left outer join MGAPMSup sup on
                    (sup.IdMSup = giro.IdMSup
                        and giro.JenisMGiro = 'K')
                left outer join MGAPTBHutDB HutDB on
                    (HutDB.IdMCabangMRef = giro.IdMCabang
                        and HutDB.IdMRef = giro.IdMGiro
                        and HutDB.JenisMRef = 'G')
                left outer join MGAPTBHut Hut on
                    (Hut.IdMCabang = HutDB.IdMCabang
                        and Hut.IdTBHut = HutDB.IdTBHut)
                where
                    (m.Hapus = 0
                        and m.Void = 0)
                    and m.JenisTGiroTolak = 'K'
                    and HutDB.JenisMRef = 'G'
                    and Hut.Void = 0
                    and Hut.Hapus = 0
            union all
                select
                    Sup.IdMSup as IdMSup ,
                    4.4 as JenisTrans ,
                    m.IdTGiroGanti as IdTrans ,
                    m.BuktiTGiroGanti as BuktiTrans ,
                    CONCAT(DATE(m.tgltgiroganti), ' ', TIME(m.tglupdate)) as tgltrans ,
                    -d.JmlBayar as JmlHut ,
                    CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')') as Keterangan
                from
                    MGKBTGiroGantiDG d
                left outer join MGKBTGiroGanti m on
                    (m.IdMCabang = d.IdMCabang
                        and m.IDTGiroGanti = d.IDTGiroGanti)
                left outer join MGKBMGiro giro on
                    (giro.IdMCabang = d.IdMCabangMGiro
                        and giro.IdMGiro = d.IdMGiro)
                left outer join MGAPMSup Sup on
                    (Sup.IdMSup = giro.IdMSup
                        and Giro.JenisMGiro = 'K')
                left outer join MGAPTBHutDB HutDB on
                    (HutDB.IdMCabangMRef = giro.IdMCabang
                        and HutDB.IdMRef = Giro.IdMGiro
                        and HutDB.JenisMREF = 'G')
                left outer join MGAPTBHut Hut on
                    (Hut.IdMCabang = HutDB.IdMCabang
                        and Hut.IdTBHut = HutDB.IdTBHut)
                where
                    (m.Hapus = 0
                        and m.Void = 0)
                    and m.JenisTGiroGanti = 'K'
                    and HutDB.JenisMRef = 'G'
                    and Hut.Void = 0
                    and Hut.Hapus = 0
            union all
                select
                    IdMSup ,
                    5 as JenisTrans ,
                    IdTKorHut as IdTrans ,
                    BuktiTKorHut as BuktiTrans ,
                    CONCAT(DATE(TglTKorHut), ' ', TIME(TglUpdate)) as TglTrans ,
                    Total as JmlHut ,
                    CONCAT('Koreksi Hutang ', BuktiTKorHut) as Keterangan
                from
                    MGAPTKorHut
                where
                    Hapus = 0
                    and Void = 0
                    and Total <> 0 ) Tbl
        union all
            select
                '${tgl2}' as TglTrans,
                IdMSup,
                0 as PosHut
            from
                MGAPMSup ) TransAll
        where
            TglTrans = '${tgl2}'
        group by
            IdMSup ) TablePosHut
    left outer join MGAPMSup MSup on
        (TablePosHut.IdMSup = MSup.IdMSup)
    where
        MSup.Hapus = 0
        and MSup.Aktif = 1`;
    return sql;
}


exports.dashboard_grafiklabarugi = async (tgl2 = "") => { 
    var sql1 = `select
        Tgl,
        sum((NilaiJual - NilaiHPP)) as jumlah
    from
        (
        select
            IdMCabang,
            Tgl,
            SUM(NilaiJual) as NilaiJual,
            SUM(NilaiHPP) as NilaiHPP,
            SUM(NilaiBeli) as NilaiBeli
        from
            (
            select
                IdMCabang as IdMCabang,
                Tgltjual as Tgl,
                SUM(Qty * HrgStn) as NilaiJual,
                SUM(Qty * HPP) as NilaiHPP,
                SUM(Qty * HrgBeliAk) as NilaiBeli
            from
                (
                select
                    m.IdMCabang,
                    m.Tgltjual,
                    (d.HrgStn - coalesce(d.DiscV, 0)) * (1 - (coalesce(m.DiscV, 0)/ m.Bruto)) as HrgStn,
                    coalesce(d.HPP, 0) as HPP,
                    d.qtyTotal as Qty,
                    beli.HrgBeliAk
                from
                    mgartjuald d
                left outer join mgartjual m on
                    (m.IdMCabang = d.IdMCabang
                        and m.Idtjual = d.Idtjual)
                left outer join (
                    select
                        MAX(TglBeliAk) as TglBeliAk,
                        MAX(HrgBeliAk) as HrgBeliAk,
                        IdMCabang,
                        IdMbrg
                    from
                        (
                        select
                            m.TglTBeli as TglBeliAk,
                            m.TglCreate,
                            m.IdMCabang,
                            d.IdMBrg,
                            coalesce(d.HrgStn * ((100 - d.DiscP) / 100) * ((100 - m.DiscP) / 100) * ((100 + 0) / 100), 0) as HrgBeliAk
                        from
                            MGAPTBeliD d
                        left outer join MGAPTBeli m on
                            (d.IdMCabang = m.IdMCabang
                                and d.IdTBeli = m.IdTBeli)
                        where
                            m.Hapus = 0
                            and m.Void = 0
                    union all
                        select
                            TglTSABrg as TglBeliAk,
                            TglCreate,
                            IdMCabang,
                            IdMBrg,
                            coalesce(HPP, 0) as HrgBeliAk
                        from
                            MGINTSABrg) TabelBeli
                    group by
                        idmcabang,
                        IdMBrg
                    order by
                        TglBeliAk desc,
                        IdMCabang,
                        IdMBrg) beli on
                    (beli.IdMCabang = m.IdMCabang
                        and beli.IdMbrg = d.IdMbrg)
                where
                    m.Hapus = 0
                    and m.Void = 0
                    and m.Tgltjual = '${tgl2}') Jual
            group by
                IdMCabang,
                Tgltjual) TblTrans
        group by
            IdMCabang,
            Tgl) tblall
    left outer join MGSYMCabang MCabang on
        (TblAll.IdMCabang = MCabang.IdMCabang)
    where
        (MCabang.Hapus = 0)`;
    
    var sql2 = `SELECT tglttransfer as tgl,  coalesce(SUM(Jumlah),0) AS Jumlah
        FROM (
            SELECT m.IdMCabang, m.tglttransfer,-SUM(JMLBAYAR) AS Jumlah
            FROM MGKBTTransferD d 
            LEFT OUTER JOIN MGKBTTransfer m ON (m.IdMCabang = d.IdMCabang AND m.IdTTransfer = d.IdTTransfer) 
            LEFT OUTER JOIN MGGLMPrk p ON (p.IdMPrk = d.IdMRef AND d.JenisMRef = 'P' AND p.Periode = 0)
            WHERE d.jenismref = 'P'
            AND m.jenisttransfer = 'K'
            AND p.jenismprkd IN (10, 11, 13)
            AND m.Hapus = 0 AND m.Void = 0 
            AND m.TglTTransfer = '${tgl2}'
            GROUP BY m.tglttransfer
            
        ) TableBiayaKasKeluar
        GROUP BY IdMCabang,tglttransfer`;
    var sql = `select tgl,sum(jumlah) as jumlah from (${sql1} union all ${sql2}) x group by tgl order by tgl`;

    return sql;
}