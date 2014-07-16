package RecommendSystem;

import java.io.UnsupportedEncodingException;

import com.sun.jna.Library;
import com.sun.jna.Native;








public class Nlpir {
	
	// 定义接口CLibrary，继承自com.sun.jna.Library
	public interface CLibrary extends Library {
		// 定义并初始化接口的静态变量 
		CLibrary Instance = (CLibrary) Native.loadLibrary(
				//"E:\\NLPIR2014\\ICTCLAS2014\\lib\\win64\\NLPIR", CLibrary.class);
				"./NLPIR", CLibrary.class);

		// printf函数声明
		public int NLPIR_Init(byte[] sDataPath, int encoding,
				byte[] sLicenceCode);

		public String NLPIR_ParagraphProcess(String sSrc, int bPOSTagged);

		public String NLPIR_GetKeyWords(String sLine, int nMaxKeyLimit,
				boolean bWeightOut);

		public void NLPIR_Exit();
	}

	public static String transString(String aidString, String ori_encoding,
			String new_encoding) {
		try {
			return new String(aidString.getBytes(ori_encoding), new_encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	static{
		String argu = ".";
		String system_charset = "UTF-8";
		int charset_type = 1;
		
		int init_flag = 0;
		try {
			init_flag = CLibrary.Instance.NLPIR_Init(argu.getBytes(system_charset),
					charset_type, "0".getBytes(system_charset));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (0 == init_flag) {
			//System.err.println("初始化失败！");
			try {
				throw new Exception("初始化失败！");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static String[] spliteword(String sInput,String charset){
		
		String token[] = null;
		String tmp[];
		String nativeBytes = null;
		try {
			nativeBytes = CLibrary.Instance.NLPIR_ParagraphProcess(sInput, 0);

			tmp = nativeBytes.split(" ");
			token = new String[tmp.length];
			int i,j;
			for(i=0,j=0;i<tmp.length;i++){
				if(!tmp[i].trim().equals(""))
					token[j++] = tmp[i].trim();
			}
			
			tmp = new String[j];
			for(i=0;i<j;i++){
				tmp[i] = token[i];
			}
			
			token = tmp;
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
		
		return token;
	}
	
	public static void exit(){
		CLibrary.Instance.NLPIR_Exit();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String sInput = "承诺。 美国当天在首都华盛顿“朝鲜战争纪念碑”前举行纪念仪式。奥巴马在仪式上发表讲话强调，美国对韩国的安全承诺“永远不会动摇”。他说，停战60周年后，韩国已经成为世界上最有活力的经济体之一，美韩伙伴关系仍然是亚太地区的“稳定基石”。 奥巴马还强调，朝鲜战争表明美国维持世界上最强大军队的必要性。 美国国防部长哈格尔在纪念仪式上表示，美国在韩国部署了大约2.8万军队，美方将确保朝鲜半岛的“和平与稳定”。他还说，朝鲜战争让美国获取的一个重要教训是，盟国和国际机构是美国影响力的“延伸”，而非对美国力量的制约，它们对美国维持和平与稳定这一长远愿景“至关重要”，尤其在亚太地区。 朝鲜战争于1950年6月25日爆发，随着1953年7月27日朝鲜停战协定签署而告结束。战争造成大约3.6万美军死亡，超过10万人受伤，另外7910名美军官兵至今失踪。 奥巴马车队当天在从白宫前往朝鲜战争纪念碑途中遭遇一群抗议者。他们打着“不要战争，要和平”等标牌。（新华社记者易爱军）";
		
		String token[] = Nlpir.spliteword(sInput, "utf-8");
		for(int i=0;i<token.length;i++){
			System.out.println(token[i]);
		}
		
		Nlpir.exit();
	}

}
