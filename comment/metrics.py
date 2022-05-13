from nltk.translate.bleu_score import sentence_bleu
from rouge import Rouge


def compute_BLEU(reference, candidate):
    one_gram_score = sentence_bleu(reference, candidate, weights=(1, 0, 0, 0))
    two_gram_score = sentence_bleu(reference, candidate, weights=(0, 1, 0, 0))
    four_gram_score = sentence_bleu(reference, candidate, weights=(0, 0, 0, 1))
    return one_gram_score, two_gram_score, four_gram_score


def compute_batch_BLEU(references, candidates):
    references = [[list(ref)] for ref in references]
    candidates = [cand.split(" ") for cand in candidates]

    bleu1_score = []
    bleu2_score = []
    bleu4_score = []
    for index in range(len(references)):
        one_gram_score, two_gram_score, four_gram_score = compute_BLEU(references[index], candidates[index])
        bleu1_score.append(one_gram_score)
        bleu2_score.append(two_gram_score)
        bleu4_score.append(four_gram_score)
    bleu1 = sum(bleu1_score) / len(bleu1_score)
    bleu2 = sum(bleu2_score) / len(bleu2_score)
    bleu4 = sum(bleu4_score) / len(bleu4_score)

    return bleu1, bleu2, bleu4


def compute_ROUGE(reference, candidate):
    rouge = Rouge()
    rouge_score = rouge.get_scores(candidate, reference)
    rouge1_score = rouge_score[0]["rouge-1"]['f']
    rouge2_score = rouge_score[0]["rouge-2"]['f']
    rougeL_score = rouge_score[0]["rouge-l"]['f']

    return rouge1_score, rouge2_score, rougeL_score


def compute_batch_ROUGE(references, candidates):
    references = [" ".join(list(ref)) for ref in references]
    rouge = Rouge()
    rouge1_score_list = []
    rouge2_score_list = []
    rougeL_score_list = []

    for index in range(len(references)):
        rouge_score = rouge.get_scores(candidates[index], references[index])
        rouge1_score = rouge_score[0]["rouge-1"]['f']
        rouge2_score = rouge_score[0]["rouge-2"]['f']
        rougeL_score = rouge_score[0]["rouge-l"]['f']
        rouge1_score_list.append(rouge1_score)
        rouge2_score_list.append(rouge2_score)
        rougeL_score_list.append(rougeL_score)

    rouge1 = sum(rouge1_score_list) / len(rouge1_score_list)
    rouge2 = sum(rouge2_score_list) / len(rouge2_score_list)
    rougeL = sum(rougeL_score_list) / len(rougeL_score_list)

    return rouge1, rouge2, rougeL


if __name__ == '__main__':
    ref = ["哈哈哈怎么回事", "哈哈哈怎么回事"]
    c1 = ["哈 哈 哈 怎 么 回 事", "怎 么 回 事"]
    one_gram_score, two_gram_score, four_gram_score = compute_batch_BLEU(ref, c1)
    rouge1_score_list, rouge2_score_list, rougeL_score_list = compute_batch_ROUGE(ref, c1)

    print("")
