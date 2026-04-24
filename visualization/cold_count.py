import pandas as pd
from io import StringIO

raw_data = """
creation_time_us  service      pod_count
----------------  -----------  ---------
1776743671309017  small-fast   0        
1776743671309017  small-fast2  0        
1776743673901881  small-fast   0        
1776743673901881  small-fast2  0        
1776743679232057  small-fast   16       
1776743679232057  small-fast2  0        
1776743684451054  small-fast   33       
1776743684451054  small-fast2  0        
1776743687610229  small-fast   33       
1776743687610229  small-fast2  0        
1776743690957933  small-fast   33       
1776743690957933  small-fast2  0        
1776743695198195  small-fast   33       
1776743695198195  small-fast2  0        
1776743698294642  small-fast   33       
1776743698294642  small-fast2  0        
1776743700927217  small-fast   33       
1776743700927217  small-fast2  0        
1776743704038474  small-fast   33       
1776743704038474  small-fast2  0        
1776743707266175  small-fast   33       
1776743707266175  small-fast2  0        
1776743709727141  small-fast   33       
1776743709727141  small-fast2  0        
1776743712121180  small-fast   33       
1776743712121180  small-fast2  0        
1776743714391898  small-fast   33       
1776743714391898  small-fast2  0        
1776743717109210  small-fast   33       
1776743717109210  small-fast2  0        
1776743719970163  small-fast   33       
1776743719970163  small-fast2  0        
1776743723596688  small-fast   33       
1776743723596688  small-fast2  0        
1776743726755955  small-fast   33       
1776743726755955  small-fast2  6        
1776743729960969  small-fast   33       
1776743729960969  small-fast2  6        
1776743732924873  small-fast   33       
1776743732924873  small-fast2  6        
1776743735110796  small-fast   33       
1776743735110796  small-fast2  6        
1776743737547368  small-fast   33       
1776743737547368  small-fast2  6        
1776743739921124  small-fast   33       
1776743739921124  small-fast2  6        
1776743742750323  small-fast   33       
1776743742750323  small-fast2  6        
1776743745121167  small-fast   33       
1776743745121167  small-fast2  6        
1776743749455971  small-fast   33       
1776743749455971  small-fast2  6        
1776743752330563  small-fast   33       
1776743752330563  small-fast2  6        
1776743754481629  small-fast   33       
1776743754481629  small-fast2  6        
1776743756938180  small-fast   33       
1776743756938180  small-fast2  6        
1776743759595810  small-fast   33       
1776743759595810  small-fast2  6        
1776743762058392  small-fast   33       
1776743762058392  small-fast2  6        
1776743764885791  small-fast   33       
1776743764885791  small-fast2  6        
1776743767472556  small-fast   33       
1776743767472556  small-fast2  6        
1776743770062055  small-fast   33       
1776743770062055  small-fast2  6        
1776743774048100  small-fast   33       
1776743774048100  small-fast2  6        
1776743776597481  small-fast   33       
1776743776597481  small-fast2  6        
1776743779190247  small-fast   33       
1776743779190247  small-fast2  6        
1776743781956187  small-fast   33       
1776743781956187  small-fast2  6        
1776743784312067  small-fast   33       
1776743784312067  small-fast2  6        
1776743786833181  small-fast   33       
1776743786833181  small-fast2  6        
1776743789486653  small-fast   33       
1776743789486653  small-fast2  6        
1776743792622638  small-fast   33       
1776743792622638  small-fast2  6        
1776743794934150  small-fast   33       
1776743794934150  small-fast2  6        
1776743797995506  small-fast   33       
1776743797995506  small-fast2  6        
1776743801035766  small-fast   33       
1776743801035766  small-fast2  6        
1776743803883698  small-fast   33       
1776743803883698  small-fast2  6        
1776743806751682  small-fast   33       
1776743806751682  small-fast2  6        
1776743810439207  small-fast   33       
1776743810439207  small-fast2  6        
1776743813830633  small-fast   33       
1776743813830633  small-fast2  6        
1776743816687388  small-fast   33       
1776743816687388  small-fast2  6        
1776743819597504  small-fast   33       
1776743819597504  small-fast2  6        
1776743822605459  small-fast   33       
1776743822605459  small-fast2  6        
1776743825299870  small-fast   33       
1776743825299870  small-fast2  6        
1776743828007182  small-fast   33       
1776743828007182  small-fast2  6        
1776743830102901  small-fast   33       
1776743830102901  small-fast2  6        
1776743832363356  small-fast   33       
1776743832363356  small-fast2  6        
1776743834525343  small-fast   33       
1776743834525343  small-fast2  6        
1776743837524376  small-fast   33       
1776743837524376  small-fast2  6        
1776743840294954  small-fast   33       
1776743840294954  small-fast2  6        
1776743842855020  small-fast   33       
1776743842855020  small-fast2  6        
1776743845510338  small-fast   33       
1776743845510338  small-fast2  6        
1776743848272589  small-fast   33       
1776743848272589  small-fast2  6        
1776743850516120  small-fast   33       
1776743850516120  small-fast2  6        
1776743853600742  small-fast   33       
1776743853600742  small-fast2  6        
1776743855866760  small-fast   33       
1776743855866760  small-fast2  6        
1776743858138375  small-fast   33       
1776743858138375  small-fast2  6        
1776743860485294  small-fast   33       
1776743860485294  small-fast2  6        
1776743862624880  small-fast   33       
1776743862624880  small-fast2  6        
1776743864740504  small-fast   33       
1776743864740504  small-fast2  6        
1776743868021872  small-fast   33       
1776743868021872  small-fast2  6        
1776743870208151  small-fast   33       
1776743870208151  small-fast2  6        
1776743873584828  small-fast   33       
1776743873584828  small-fast2  6        
1776743875924287  small-fast   33       
1776743875924287  small-fast2  6        
1776743878092520  small-fast   33       
1776743878092520  small-fast2  6        
1776743880701815  small-fast   33       
1776743880701815  small-fast2  6        
1776743883064194  small-fast   33       
1776743883064194  small-fast2  6        
1776743885143685  small-fast   33       
1776743885143685  small-fast2  6        
1776743887868650  small-fast   33       
1776743887868650  small-fast2  6        
1776743890254922  small-fast   33       
1776743890254922  small-fast2  6        
1776743894435876  small-fast   33       
1776743894435876  small-fast2  6        
1776743897501774  small-fast   33       
1776743897501774  small-fast2  6        
1776743899954231  small-fast   33       
1776743899954231  small-fast2  6        
1776743902649879  small-fast   33       
1776743902649879  small-fast2  6        
1776743904808714  small-fast   33       
1776743904808714  small-fast2  6        
1776743907445141  small-fast   33       
1776743907445141  small-fast2  6        
1776743909617726  small-fast   33       
1776743909617726  small-fast2  6        
1776743912229468  small-fast   33       
1776743912229468  small-fast2  6        
1776743914388914  small-fast   33       
1776743914388914  small-fast2  6        
1776743916857896  small-fast   33       
1776743916857896  small-fast2  6        
1776743919940589  small-fast   33       
1776743919940589  small-fast2  6        
1776743922683078  small-fast   33       
1776743922683078  small-fast2  6        
1776743925094683  small-fast   33       
1776743925094683  small-fast2  6        
1776743928833000  small-fast   33       
1776743928833000  small-fast2  6        
1776743932624710  small-fast   33       
1776743932624710  small-fast2  6        
1776743938295693  small-fast   33       
1776743938295693  small-fast2  6        
1776743941048253  small-fast   33       
1776743941048253  small-fast2  6        
1776743944217515  small-fast   33       
1776743944217515  small-fast2  6        
1776743946814128  small-fast   33       
1776743946814128  small-fast2  6        
1776743949302443  small-fast   33       
1776743949302443  small-fast2  6        
1776743952080575  small-fast   33       
1776743952080575  small-fast2  6        
1776743954312268  small-fast   33       
1776743954312268  small-fast2  6        
1776743957574629  small-fast   33       
1776743957574629  small-fast2  6        
1776743961016967  small-fast   33       
1776743961016967  small-fast2  6        
1776743964521373  small-fast   33       
1776743964521373  small-fast2  6        
1776743967435188  small-fast   33       
1776743967435188  small-fast2  6        
1776743969838828  small-fast   33       
1776743969838828  small-fast2  6        
1776743972953508  small-fast   33       
1776743972953508  small-fast2  6        
1776743975608374  small-fast   33       
1776743975608374  small-fast2  6        
1776743978398671  small-fast   33       
1776743978398671  small-fast2  6        
1776743982489932  small-fast   33       
1776743982489932  small-fast2  6        
1776743984603990  small-fast   33       
1776743984603990  small-fast2  6        
1776743987611813  small-fast   33       
1776743987611813  small-fast2  6        
1776743990186357  small-fast   33       
1776743990186357  small-fast2  6        
1776743993563292  small-fast   33       
1776743993563292  small-fast2  6        
1776743996970890  small-fast   33       
1776743996970890  small-fast2  6        
1776744000290852  small-fast   33       
1776744000290852  small-fast2  6        
1776744006188582  small-fast   33       
1776744006188582  small-fast2  6        
1776744008794152  small-fast   33       
1776744008794152  small-fast2  6        
1776744011141103  small-fast   33       
1776744011141103  small-fast2  6        
1776744013347802  small-fast   33       
1776744013347802  small-fast2  6        
1776744015498521  small-fast   33       
1776744015498521  small-fast2  6        
1776744018080507  small-fast   33       
1776744018080507  small-fast2  6        
1776744021004247  small-fast   33       
1776744021004247  small-fast2  6        
1776744023673875  small-fast   33       
1776744023673875  small-fast2  6        
1776744027449495  small-fast   33       
1776744027449495  small-fast2  6        
1776744030221566  small-fast   33       
1776744030221566  small-fast2  6        
1776744032803356  small-fast   33       
1776744032803356  small-fast2  6        
1776744035036311  small-fast   33       
1776744035036311  small-fast2  6        
1776744037397378  small-fast   33       
1776744037397378  small-fast2  6        
1776744039555327  small-fast   33       
1776744039555327  small-fast2  6        
1776744042467401  small-fast   33   
"""

def count_cold_starts_by_service(raw_data: str):
    lines = raw_data.strip().splitlines()

    # 구분선/헤더 제거
    data_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("creation_time_us"):
            continue
        if set(line.replace(" ", "")) == {"-"}:
            continue
        data_lines.append(line)

    df = pd.read_csv(
        StringIO("\n".join(data_lines)),
        sep=r"\s+",
        names=["creation_time_us", "service", "pod_count"]
    )

    df["creation_time_us"] = pd.to_numeric(df["creation_time_us"], errors="coerce")
    df["pod_count"] = pd.to_numeric(df["pod_count"], errors="coerce").fillna(0).astype(int)

    results = {}

    for service, group in df.groupby("service"):
        group = group.sort_values("creation_time_us")

        prev = None
        total = 0

        for curr in group["pod_count"]:
            if prev is not None and curr > prev:
                total += (curr - prev)
            prev = curr

        results[service] = total

    return results


result = count_cold_starts_by_service(raw_data)
print(result)