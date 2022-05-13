import os

os.environ["CUDA_VISIBLE_DEVICES"] = "5"
import math
import torch
import pickle
from torch.utils.data import DataLoader
from transformers.modeling_outputs import Seq2SeqModelOutput, Seq2SeqLMOutput
from local_bart_modeling import BartForConditionalGeneration
from transformers import BertTokenizer, BartConfig, AdamW, get_scheduler
import random
import logging
from tqdm import tqdm
import time
from metrics import compute_batch_BLEU, compute_batch_ROUGE

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)


def squeeze_tables(data):
    max_table_len = 0
    for idx, item in tqdm(enumerate(data)):
        market_data = item["market_data"]  # sh, sz
        sh_data = market_data["sh"]
        sz_data = market_data["sz"]
        squeeze_sh = f"{item['time']}；上证指数；开盘：{sh_data.open.item()}；收盘：{sh_data.close.item()}；" \
                     f"最高：{sh_data.high.item()}；最低：{sh_data.low.item()}；" \
                     f"涨跌点：{sh_data.change.item()}；涨跌幅：{sh_data.pct_chg.item()}；"
        squeeze_sz = f"{item['time']}；深证指数；开盘：{sz_data.open.item()}；收盘：{sz_data.close.item()}；" \
                     f"最高：{sz_data.high.item()}；最低：{sz_data.low.item()}；" \
                     f"涨跌点：{sz_data.change.item()}；涨跌幅：{sz_data.pct_chg.item()}；"
        item["squeeze_sh"] = squeeze_sh
        item["squeeze_sz"] = squeeze_sz

        max_table_len = max(len(squeeze_sh), max_table_len)
        max_table_len = max(len(squeeze_sz), max_table_len)

        stock_data = item["stock_data"]
        squeeze_stock_data = dict()
        for stock_name, sd in stock_data.items():
            if len(sd) == 0:
                continue
            # print(sd)
            squeeze_stock = f"{item['time']}；股票：{stock_name}；开盘：{sd.open.item()}；收盘：{sd.close.item()}；" \
                            f"最高：{sd.high.item()}；最低：{sd.low.item()}；" \
                            f"涨跌额：{sd.change.item()}；涨跌幅：{sd.pct_chg.item()}"
            squeeze_stock_data[stock_name] = squeeze_stock

            max_table_len = max(len(squeeze_stock), max_table_len)

        item["squeeze_stock_data"] = squeeze_stock_data

        industry_data = item["industry_data"]
        squeeze_industry_data = dict()
        for industry_name, sd in industry_data.items():
            if len(sd) == 0:
                continue
            squeeze_industry = f"{item['time']}；行业：{industry_name}；开盘：{sd.open.item()}；收盘：{sd.close.item()}；" \
                               f"最高：{sd.high.item()}；最低：{sd.low.item()}；" \
                               f"涨跌额：{sd.change.item()}；涨跌幅：{sd.change.item() / sd.open.item() * 100:.3f}"
            squeeze_industry_data[industry_name] = squeeze_industry

            max_table_len = max(len(squeeze_industry), max_table_len)

        item["squeeze_industry_data"] = squeeze_industry_data

    logging.info(f"squeezed tables, max squeeze length = {max_table_len}")
    return data


def load_dataset(stock_table_num=8, industry_table_num=4):
    data_path = "/mnt/inspurfs/user-fs/zhaoyu/workspace/StockMarketSummary/annotate_day_data_0512.pkl"
    # data_path = "./small_debug_data.pkl"
    data = pickle.load(open(data_path, 'rb'))
    # pickle.dump(data[:100], open("./small_debug_data.pkl", 'wb'))
    if "squeeze_stock_data" not in data[0]:
        data = squeeze_tables(data)
        pickle.dump(data, open(data_path, 'wb'))

    for item in data:
        item["tables"] = [item["squeeze_sh"], item["squeeze_sz"]] + \
                         list(item["squeeze_stock_data"].values())[:stock_table_num] + \
                         list(item["squeeze_industry_data"].values())[:industry_table_num]
    dev = data[:300]
    train = data[250:]
    # train = data[:48]
    # dev = data[:48]
    return train, dev


@torch.no_grad()
def evaluation(model, tokenizer, dataset, dataloader):
    model.eval()

    max_target_length = 500
    gen_kwargs = {"max_length": max_target_length,
                  "num_beams": None, }

    all_gen = []
    reference = []
    for batch in tqdm(dataloader):
        generated_tokens = model.generate(
            input_ids=batch["input_ids"].to(model.device),
            attention_mask=batch["attention_mask"].to(model.device),
            **gen_kwargs,
        )
        decoded_tokens = tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)
        all_gen += decoded_tokens

    reference += [item["text_post"][:max_target_length] for item in dataset]

    rouge1, rouge2, rougel = compute_batch_ROUGE(reference, all_gen)
    bleu1, bleu2, bleu4 = compute_batch_BLEU(reference, all_gen)

    return {"rouge": [rouge1 * 100, rouge2 * 100, rougel * 100],
            "bleu": [bleu1 * 100, bleu2 * 100, bleu4 * 100]}, all_gen


def save_model(model, save_dir, tokenizer=None, pred_res=None):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    model.save_pretrained(save_dir)
    if tokenizer is not None:
        tokenizer.save_pretrained(save_dir)
    if pred_res is not None:
        pickle.dump(pred_res, open(os.path.join(save_dir, "pred_res.pkl"), 'wb'))


def main():
    logging.info("Comment Generation Training Script.")
    logging.info(f"cuda available: {torch.cuda.is_available()}")
    table_max_length = 103  # include cls and sep
    stock_table_num = 8
    industry_table_num = 4
    table_num = 2 + stock_table_num + industry_table_num

    train_data, dev_data = load_dataset(stock_table_num=stock_table_num,
                                        industry_table_num=industry_table_num)
    logging.info(f"Loaded dataset. train: {len(train_data)}, dev:{len(dev_data)}")

    bart_base_path = "/mnt/inspurfs/user-fs/datasets/StockMarketSummary/bart-base-chinese/"
    model = BartForConditionalGeneration.from_pretrained(bart_base_path)

    if torch.cuda.is_available():
        model = model.cuda()

    logging.info("Loaded model.")

    config = BartConfig.from_pretrained(bart_base_path)
    tokenizer = BertTokenizer.from_pretrained("fnlp/bart-base-chinese")
    vocab = tokenizer.vocab
    unk_token_id, pad_token_id = tokenizer.unk_token_id, tokenizer.pad_token_id
    cls_token_id, sep_token_id = tokenizer.cls_token_id, tokenizer.sep_token_id
    pad_table_seq = [pad_token_id] * table_max_length

    def process_input_data(examples):
        batch_table_inputs = []
        for ex in examples:
            ex_tables_inputs = []
            for table in ex["tables"]:
                table_ids = [cls_token_id]
                for c in table:
                    table_ids.append(vocab.get(c, unk_token_id))
                table_ids.append(sep_token_id)
                table_ids += [pad_token_id] * (table_max_length - len(table_ids))
                ex_tables_inputs.append(table_ids)
            ex_tables_inputs += (table_num - len(ex["tables"])) * [pad_table_seq]
            assert len(ex_tables_inputs) == table_num
            assert all(len(t) == table_max_length for t in ex_tables_inputs)
            batch_table_inputs.append(ex_tables_inputs)

        batch_table_inputs = torch.tensor(batch_table_inputs)
        batch_table_mask = (batch_table_inputs != 0).long()

        targets = [ex['text_post'] for ex in examples]

        with tokenizer.as_target_tokenizer():
            targets = tokenizer(targets, max_length=500, padding=True, truncation=True, return_tensors="pt")

        return {
            "input_ids": batch_table_inputs,
            "attention_mask": batch_table_mask,
            "labels": torch.cat([targets["input_ids"][:, 1:],
                                 torch.tensor([[pad_token_id]] * targets["input_ids"].shape[0])], dim=1),
            "decoder_input_ids": targets["input_ids"],
            "decoder_attention_mask": targets["attention_mask"],
        }

    batch_size = 14
    eval_batch_size = 24

    train_dataloader = DataLoader(train_data, shuffle=True, collate_fn=process_input_data,
                                  batch_size=batch_size, num_workers=0)

    dev_dataloader = DataLoader(dev_data, shuffle=False, collate_fn=process_input_data,
                                batch_size=eval_batch_size, num_workers=0)

    weight_decay = 0.0
    learning_rate = 4e-5
    gradient_accumulation_steps = 1
    num_train_epochs = 30
    lr_scheduler_type = "constant"
    num_warmup_steps = 0
    save_dir = os.path.join(f"./model_save", f"{time.strftime('%m-%d')}")

    no_decay = ["bias", "LayerNorm.weight"]
    optimizer_grouped_parameters = [
        {"params": [p for n, p in model.named_parameters() if not any(nd in n for nd in no_decay)],
         "weight_decay": weight_decay, },
        {"params": [p for n, p in model.named_parameters() if any(nd in n for nd in no_decay)],
         "weight_decay": 0.0, },
    ]
    optimizer = AdamW(optimizer_grouped_parameters, lr=learning_rate)

    # Scheduler and math around the number of training steps.
    num_update_steps_per_epoch = math.ceil(len(train_dataloader) / gradient_accumulation_steps)
    max_train_steps = num_train_epochs * num_update_steps_per_epoch

    lr_scheduler = get_scheduler(
        name=lr_scheduler_type,
        optimizer=optimizer,
        num_warmup_steps=num_warmup_steps,
        num_training_steps=max_train_steps,
    )
    progress_bar = tqdm(range(max_train_steps), total=max_train_steps)
    completed_steps = 0
    best_score = float("-inf")

    for epoch in range(num_train_epochs):
        for step, batch in enumerate(train_dataloader):
            model.train()
            model_outputs: Seq2SeqLMOutput = model(
                input_ids=batch["input_ids"].to(model.device),
                attention_mask=batch["attention_mask"].to(model.device),
                labels=batch["labels"].to(model.device),
                decoder_input_ids=batch["decoder_input_ids"].to(model.device),
                decoder_attention_mask=batch["decoder_attention_mask"].to(model.device),
                return_dict=True
            )
            loss = model_outputs.loss
            loss = loss / gradient_accumulation_steps
            loss.backward()
            if step % gradient_accumulation_steps == 0 or step == len(train_dataloader) - 1:
                optimizer.step()
                lr_scheduler.step()
                optimizer.zero_grad()
                progress_bar.update(1)
                completed_steps += 1
                progress_bar.set_description(f"step[{completed_steps}] Loss[{loss:.5f}]")

            if completed_steps >= max_train_steps:
                break

        if epoch >= 4:
            logging.info("Evaluate")
            eval_scores, pred_res = evaluation(model, tokenizer, dev_data, dev_dataloader)
            rouge_str = f"Rouge1[{eval_scores['rouge'][0]:.3f}] Rouge2[{eval_scores['rouge'][1]:.3f}] RougeL[{eval_scores['rouge'][2]:.3f}]"
            bleu_str = f"BLEU1[{eval_scores['bleu'][0]:.3f}] BLEU2[{eval_scores['bleu'][1]:.3f}] BLEU4[{eval_scores['bleu'][2]:.3f}]"

            logging.info(f"eval scores: {rouge_str} {bleu_str}")
            eval_score = eval_scores["bleu"][0]
            if eval_score > best_score:
                best_score = eval_score
                logging.info(f"Save model, best score: BLEU1: {best_score:.3f}")
                save_model(model, save_dir, tokenizer=tokenizer, pred_res=pred_res)


if __name__ == '__main__':
    main()
