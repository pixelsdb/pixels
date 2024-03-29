package io.pixelsdb.pixels.parser;

import java.lang.reflect.Field;

public class ClickbenchQuery
{
    public static final String Q1 = "SELECT COUNT(*) FROM hits";
    public static final String Q2 = "SELECT COUNT(*) FROM hits WHERE advengineid <> 0";
    public static final String Q3 = "SELECT SUM(advengineid), COUNT(*), AVG(resolutionwidth) FROM hits";
    public static final String Q4 = "SELECT AVG(userid) FROM hits";
    public static final String Q5 = "SELECT COUNT(DISTINCT userid) FROM hits";
    public static final String Q6 = "SELECT COUNT(DISTINCT searchphrase) FROM hits";
    public static final String Q7 = "SELECT MIN(eventdate), MAX(eventdate) FROM hits";
    public static final String Q8 = "SELECT advengineid, COUNT(*) FROM hits WHERE advengineid <> 0 GROUP BY advengineid ORDER BY COUNT(*) DESC";
    public static final String Q9 = "SELECT regionid, COUNT(DISTINCT userid) AS u FROM hits GROUP BY regionid ORDER BY u DESC LIMIT 10";
    public static final String Q10 = "SELECT regionid, SUM(advengineid), COUNT(*) AS c, AVG(resolutionwidth), COUNT(DISTINCT userid) FROM hits GROUP BY regionid ORDER BY c DESC LIMIT 10";
    public static final String Q11 = "SELECT mobilephonemodel, COUNT(DISTINCT userid) AS u FROM hits WHERE mobilephonemodel <> '' GROUP BY mobilephonemodel ORDER BY u DESC LIMIT 10";
    public static final String Q12 = "SELECT mobilephone, mobilephonemodel, COUNT(DISTINCT userid) AS u FROM hits WHERE mobilephonemodel <> '' GROUP BY mobilephone, mobilephonemodel ORDER BY u DESC LIMIT 10";
    public static final String Q13 = "SELECT searchphrase, COUNT(*) AS c FROM hits WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10";
    public static final String Q14 = "SELECT searchphrase, COUNT(DISTINCT userid) AS u FROM hits WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY u DESC LIMIT 10";
    public static final String Q15 = "SELECT searchengineid, searchphrase, COUNT(*) AS c FROM hits WHERE searchphrase <> '' GROUP BY searchengineid, searchphrase ORDER BY c DESC LIMIT 10";
    public static final String Q16 = "SELECT userid, COUNT(*) FROM hits GROUP BY userid ORDER BY COUNT(*) DESC LIMIT 10";
    public static final String Q17 = "SELECT userid, searchphrase, COUNT(*) FROM hits GROUP BY userid, searchphrase ORDER BY COUNT(*) DESC LIMIT 10";
    public static final String Q18 = "SELECT userid, searchphrase, COUNT(*) FROM hits GROUP BY userid, searchphrase LIMIT 10";
    public static final String Q19 = "SELECT userid, extract(minute FROM eventtime) AS m, searchphrase, COUNT(*) FROM hits GROUP BY userid, extract(minute FROM eventtime), searchphrase ORDER BY COUNT(*) DESC LIMIT 10";
    public static final String Q20 = "SELECT userid FROM hits WHERE userid = 435090932899640449";
    public static final String Q21 = "SELECT COUNT(*) FROM hits WHERE url LIKE '%google%'";
    public static final String Q22 = "SELECT searchphrase, MIN(url), COUNT(*) AS c FROM hits WHERE url LIKE '%google%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10";
    public static final String Q23 = "SELECT searchphrase, MIN(url), MIN(title), COUNT(*) AS c, COUNT(DISTINCT userid) FROM hits WHERE title LIKE '%google%' AND url NOT LIKE '%.google.%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10";
    public static final String Q24 = "SELECT * FROM hits WHERE url LIKE '%google%' ORDER BY eventtime LIMIT 10";
    public static final String Q25 = "SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY eventtime LIMIT 10";
    public static final String Q26 = "SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY searchphrase LIMIT 10";
    public static final String Q27 = "SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY eventtime, searchphrase LIMIT 10";
    public static final String Q30 = "SELECT SUM(resolutionwidth), SUM(resolutionwidth + 1), SUM(resolutionwidth + 2), SUM(resolutionwidth + 3), SUM(resolutionwidth + 4), SUM(resolutionwidth + 5), SUM(resolutionwidth + 6), SUM(resolutionwidth + 7), SUM(resolutionwidth + 8), SUM(resolutionwidth + 9), SUM(resolutionwidth + 10), SUM(resolutionwidth + 11), SUM(resolutionwidth + 12), SUM(resolutionwidth + 13), SUM(resolutionwidth + 14), SUM(resolutionwidth + 15), SUM(resolutionwidth + 16), SUM(resolutionwidth + 17), SUM(resolutionwidth + 18), SUM(resolutionwidth + 19), SUM(resolutionwidth + 20), SUM(resolutionwidth + 21), SUM(resolutionwidth + 22), SUM(resolutionwidth + 23), SUM(resolutionwidth + 24), SUM(resolutionwidth + 25), SUM(resolutionwidth + 26), SUM(resolutionwidth + 27), SUM(resolutionwidth + 28), SUM(resolutionwidth + 29), SUM(resolutionwidth + 30), SUM(resolutionwidth + 31), SUM(resolutionwidth + 32), SUM(resolutionwidth + 33), SUM(resolutionwidth + 34), SUM(resolutionwidth + 35), SUM(resolutionwidth + 36), SUM(resolutionwidth + 37), SUM(resolutionwidth + 38), SUM(resolutionwidth + 39), SUM(resolutionwidth + 40), SUM(resolutionwidth + 41), SUM(resolutionwidth + 42), SUM(resolutionwidth + 43), SUM(resolutionwidth + 44), SUM(resolutionwidth + 45), SUM(resolutionwidth + 46), SUM(resolutionwidth + 47), SUM(resolutionwidth + 48), SUM(resolutionwidth + 49), SUM(resolutionwidth + 50), SUM(resolutionwidth + 51), SUM(resolutionwidth + 52), SUM(resolutionwidth + 53), SUM(resolutionwidth + 54), SUM(resolutionwidth + 55), SUM(resolutionwidth + 56), SUM(resolutionwidth + 57), SUM(resolutionwidth + 58), SUM(resolutionwidth + 59), SUM(resolutionwidth + 60), SUM(resolutionwidth + 61), SUM(resolutionwidth + 62), SUM(resolutionwidth + 63), SUM(resolutionwidth + 64), SUM(resolutionwidth + 65), SUM(resolutionwidth + 66), SUM(resolutionwidth + 67), SUM(resolutionwidth + 68), SUM(resolutionwidth + 69), SUM(resolutionwidth + 70), SUM(resolutionwidth + 71), SUM(resolutionwidth + 72), SUM(resolutionwidth + 73), SUM(resolutionwidth + 74), SUM(resolutionwidth + 75), SUM(resolutionwidth + 76), SUM(resolutionwidth + 77), SUM(resolutionwidth + 78), SUM(resolutionwidth + 79), SUM(resolutionwidth + 80), SUM(resolutionwidth + 81), SUM(resolutionwidth + 82), SUM(resolutionwidth + 83), SUM(resolutionwidth + 84), SUM(resolutionwidth + 85), SUM(resolutionwidth + 86), SUM(resolutionwidth + 87), SUM(resolutionwidth + 88), SUM(resolutionwidth + 89) FROM hits";
    public static final String Q31 = "SELECT searchengineid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits WHERE searchphrase <> '' GROUP BY searchengineid, clientip ORDER BY c DESC LIMIT 10";
    public static final String Q32 = "SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits WHERE searchphrase <> '' GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10";
    public static final String Q33 = "SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10";
    public static final String Q34 = "SELECT url, COUNT(*) AS c FROM hits GROUP BY url ORDER BY c DESC LIMIT 10";
    public static final String Q35 = "SELECT 1, url, COUNT(*) AS c FROM hits GROUP BY 1, url ORDER BY c DESC LIMIT 10";
    public static final String Q36 = "SELECT clientip, clientip - 1, clientip - 2, clientip - 3, COUNT(*) AS c FROM hits GROUP BY clientip, clientip - 1, clientip - 2, clientip - 3 ORDER BY c DESC LIMIT 10";
    public static final String Q37 = "SELECT url, COUNT(*) AS pageviews FROM hits WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND dontcounthits = 0 AND isrefresh = 0 AND url <> '' GROUP BY url ORDER BY pageviews DESC LIMIT 10";
    public static final String Q38 = "SELECT title, COUNT(*) AS pageviews FROM hits WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND dontcounthits = 0 AND isrefresh = 0 AND title <> '' GROUP BY title ORDER BY pageviews DESC LIMIT 10";
    public static final String Q39 = "SELECT url, COUNT(*) AS pageviews FROM hits WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 AND islink <> 0 AND isdownload = 0 GROUP BY url ORDER BY pageviews DESC OFFSET 1000 ROWS FETCH NEXT 10 ROWS ONLY";
    public static final String Q40 = "SELECT traficsourceid, searchengineid, advengineid, CASE WHEN (searchengineid = 0 AND advengineid = 0) THEN referer ELSE '' END AS src, url AS dst, COUNT(*) AS pageviews FROM hits WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 GROUP BY traficsourceid, searchengineid, advengineid, CASE WHEN (searchengineid = 0 AND advengineid = 0) THEN referer ELSE '' END, url ORDER BY pageviews DESC OFFSET 1000 ROWS FETCH NEXT 10 ROWS ONLY";
    public static final String Q41 = "SELECT urlhash, eventdate, COUNT(*) AS pageviews FROM hits WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 AND traficsourceid IN (-1, 6) AND refererhash = 3594120000172545465 GROUP BY urlhash, eventdate ORDER BY pageviews DESC OFFSET 1000 ROWS FETCH NEXT 10 ROWS ONLY";
    public static final String Q42 = "SELECT windowclientwidth, windowclientheight, COUNT(*) AS pageviews FROM hits WHERE counterid = 62 AND eventdate >= DATE '2013-07-01' AND eventdate <= DATE '2013-07-31' AND isrefresh = 0 AND dontcounthits = 0 AND urlhash = 2868770270353813622 GROUP BY windowclientwidth, windowclientheight ORDER BY pageviews DESC OFFSET 1000 ROWS FETCH NEXT 10 ROWS ONLY";

    public static String getQuery(int i) throws NoSuchFieldException, IllegalAccessException
    {
        Field field = ClickbenchQuery.class.getDeclaredField("Q" + i);
        Object value = field.get(null);
        return value.toString();
    }
}
