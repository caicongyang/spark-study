package com.caicongyang.spark.study.utils;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Header;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import java.io.ByteArrayInputStream;
import java.util.Enumeration;
import java.util.IdentityHashMap;

/**
 * @author caicongyang
 * @version id: MailParserUtil, v 0.1 18/3/6 下午3:06 caicongyang1 Exp $$
 */
public class MailParserUtil {

    public static final Logger logger = LoggerFactory.getLogger(MailParserUtil.class);


    public static String parse(String mailHeader) {
        IdentityHashMap<String, String> headers = new IdentityHashMap<>();
        if (StringUtils.isNotBlank(mailHeader)) {
            mailHeader = StringEscapeUtils.unescapeHtml(mailHeader);
            if (effectiveMailHeader(mailHeader)) {
                headers.put("error", "error");
                return GsonUtils.toJson(headers);
            }
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(mailHeader.getBytes());
            Session session = Session.getDefaultInstance(System.getProperties(), null);
            try {
                MimeMessage mimeMessage = new MimeMessage(session, byteArrayInputStream);
                //直接获取所有邮件头信息返回
                Enumeration<Header> enu = mimeMessage.getAllHeaders();
                while (enu.hasMoreElements()) {
                    Header item = enu.nextElement();
                    headers.put(item.getName(), item.getValue().replaceAll("\\r\\n\\t"," "));
                }
                return GsonUtils.toJson(headers);
            } catch (MessagingException e) {
                logger.error("parser error", e);
            }
        }
        headers.put("null", "null");
        return GsonUtils.toJson(headers);

    }


    public static Boolean effectiveMailHeader(String meailHeader) {
        if (meailHeader.contains("<!—cgi exception-->") || meailHeader.contains("<h1>500 Error</h1>") || meailHeader
            .contains("<HEAD>:<HEAD>") || meailHeader.contains("FS_MS_N01") || meailHeader.contains("errtxt")
            || meailHeader.contains("errorcode")) {
            return true;
        }
        return false;
    }
}
