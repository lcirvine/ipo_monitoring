SELECT 
    md.iconum
    ,agnt.as_reported_name AS [Company Name]
    ,md.id AS master_deal
    ,CAST(ms.cusip AS VARCHAR) AS CUSIP
    ,CAST(md.id AS VARCHAR) + '.NI' AS client_deal_id
    ,secl.ticker
    ,ex.[description] as exchange
    ,secis.offering_price as Price
    ,secis.min_offering_price
    ,secis.max_offering_price
    ,secdt.announcement_date
    ,secdt.pricing_date
    ,secdt.trading_date
    ,secdt.closing_date
    ,lps.[description] as deal_status
    ,dl.last_updated_date_utc
FROM
    pipe.v_tc_pipe_deal dl WITH(NOLOCK)
    INNER JOIN pipe.tc_pipe_master_deal md WITH(NOLOCK) ON dl.master_deal = md.id
    INNER JOIN pipe.tc_pipe_deal_issuer dlisr WITH(NOLOCK) ON dl.id = dlisr.deal
    INNER JOIN pipe.tc_pipe_deal_agent dlagnt WITH(NOLOCK) ON dlisr.pipe_agent = dlagnt.id
    INNER JOIN dbo.tc_agents agnt WITH(NOLOCK) ON dlagnt.agent = agnt.agent_id
    INNER JOIN pipe.tc_pipe_security sec WITH(NOLOCK) ON dl.id = sec.deal
    INNER JOIN pipe.tc_pipe_security_details secd WITH(NOLOCK) ON sec.id = secd.[security] AND secd.ipo_flag = 1
    INNER JOIN pipe.tc_pipe_master_security ms WITH(NOLOCK) ON sec.master_security = ms.id
    INNER JOIN pipe.tc_pipe_security_dates secdt WITH(NOLOCK) ON sec.id = secdt.[security]
    LEFT JOIN pipe.tc_pipe_security_listing secl WITH(NOLOCK) ON sec.id = secl.[security]
    INNER JOIN pipe.tc_pipe_security_issuance_subscription secis WITH(NOLOCK) ON sec.id = secis.[security]
    INNER JOIN pipe.tc_pipe_lookup_placement_status lps WITH(NOLOCK) ON secd.placement_status = lps.id
    LEFT JOIN dbo.SecmasExchanges ex WITH(NOLOCK) ON secl.exchange = ex.exchange_code
WHERE
    dl.rn = 1
    AND dl.last_updated_date_utc > DATEADD(DAY, -7, GETDATE())
ORDER BY secdt.announcement_date DESC