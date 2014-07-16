package RecommendSystem;

import java.io.UnsupportedEncodingException;

import com.sun.jna.Library;
import com.sun.jna.Native;








public class Nlpir {
	
	// ����ӿ�CLibrary���̳���com.sun.jna.Library
	public interface CLibrary extends Library {
		// ���岢��ʼ���ӿڵľ�̬���� 
		CLibrary Instance = (CLibrary) Native.loadLibrary(
				//"E:\\NLPIR2014\\ICTCLAS2014\\lib\\win64\\NLPIR", CLibrary.class);
				"./NLPIR", CLibrary.class);

		// printf��������
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
			//System.err.println("��ʼ��ʧ�ܣ�");
			try {
				throw new Exception("��ʼ��ʧ�ܣ�");
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
		String sInput = "��ŵ�� �����������׶���ʢ�١�����ս�������ǰ���м�����ʽ���°�������ʽ�Ϸ�����ǿ���������Ժ����İ�ȫ��ŵ����Զ���ᶯҡ������˵��ͣս60����󣬺����Ѿ���Ϊ���������л����ľ�����֮һ����������ϵ��Ȼ����̫�����ġ��ȶ���ʯ���� �°���ǿ��������ս����������ά����������ǿ����ӵı�Ҫ�ԡ� ������������������ڼ�����ʽ�ϱ�ʾ�������ں��������˴�Լ2.8����ӣ�������ȷ�����ʰ뵺�ġ���ƽ���ȶ���������˵������ս����������ȡ��һ����Ҫ��ѵ�ǣ��˹��͹��ʻ���������Ӱ�����ġ����족�����Ƕ�������������Լ�����Ƕ�����ά�ֺ�ƽ���ȶ���һ��ԶԸ����������Ҫ������������̫������ ����ս����1950��6��25�ձ���������1953��7��27�ճ���ͣսЭ��ǩ����������ս����ɴ�Լ3.6����������������10�������ˣ�����7910�������ٱ�����ʧ�١� �°����ӵ����ڴӰ׹�ǰ������ս�����;������һȺ�����ߡ����Ǵ��š���Ҫս����Ҫ��ƽ���ȱ��ơ����»�������װ�����";
		
		String token[] = Nlpir.spliteword(sInput, "utf-8");
		for(int i=0;i<token.length;i++){
			System.out.println(token[i]);
		}
		
		Nlpir.exit();
	}

}
