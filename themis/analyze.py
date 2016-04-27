import itertools

import pandas

from themis import CsvFileType, QUESTION, ANSWER, CONFIDENCE, IN_PURVIEW, CORRECT, FREQUENCY, logger

SYSTEM = "System"


def system_similarity(systems_data):
    """
    For each system pair, return the number of questions they answered the same.

    :param systems_data: collated results for all systems
    :type systems_data: pandas.DataFrame
    :return: table of pairs of systems and their similarity statistics
    :rtype: pandas.DataFrame
    """
    systems_data = drop_missing(systems_data)
    systems = systems_data[SYSTEM].drop_duplicates().sort_values()
    columns = ["System 1", "System 2", "Same Answer", "Same Answer %"]
    results = pandas.DataFrame(columns=columns)
    for x, y in itertools.combinations(systems, 2):
        data_x = systems_data[systems_data[SYSTEM] == x]
        data_y = systems_data[systems_data[SYSTEM] == y]
        m = pandas.merge(data_x, data_y, on=QUESTION)
        n = len(m)
        logger.info("%d question/answer pairs in common for %s and %s" % (n, x, y))
        same_answer = sum(m["%s_x" % ANSWER] == m["%s_y" % ANSWER])
        same_answer_pct = 100.0 * same_answer / n
        results = results.append(
            pandas.DataFrame([[x, y, same_answer, same_answer_pct]], columns=columns))
    results["Same Answer"] = results["Same Answer"].astype("int64")
    return results.set_index(["System 1", "System 2"])


def compare_systems(systems_data, x, y, comparison_type):
    """
    On which questions did system x do better or worse than system y?

    System x did better than system y if it correctly answered a question when system y did not, and vice versa.

    :param systems_data: collated results for all systems
    :type systems_data: pandas.DataFrame
    :param x: system name
    :type x: str
    :param y: system name
    :type y: str
    :param comparison_type: "better" or "worse"
    :type comparison_type: str
    :return: all question/answer pairs from system x that were either better or worse than system y
    :rtype: pandas.DataFrame
    """

    def col_name(type, system):
        return type + " " + system

    systems_data = drop_missing(systems_data)
    systems_data = systems_data[systems_data[IN_PURVIEW]]
    data_x = systems_data[systems_data[SYSTEM] == x]
    data_y = systems_data[systems_data[SYSTEM] == y][[QUESTION, ANSWER, CONFIDENCE, CORRECT]]
    questions = pandas.merge(data_x, data_y, on=QUESTION, how="left", suffixes=(" " + x, " " + y)).dropna()
    n = len(questions)
    logger.info("%d shared question/answer pairs between %s and %s" % (n, x, y))
    x_correct = col_name(CORRECT, x)
    y_correct = col_name(CORRECT, y)
    if comparison_type == "better":
        a = questions[x_correct] == True
        b = questions[y_correct] == False
    elif comparison_type == "worse":
        a = questions[x_correct] == False
        b = questions[y_correct] == True
    else:
        raise ValueError("Invalid comparison type %s" % comparison_type)
    d = questions[a & b]
    m = len(d)
    logger.info("%d %s (%0.3f%%)" % (m, comparison_type, 100.0 * m / n))
    d = d[[QUESTION, FREQUENCY,
           col_name(ANSWER, x), col_name(CONFIDENCE, x), col_name(ANSWER, y), col_name(CONFIDENCE, y)]]
    d = d.sort_values([col_name(CONFIDENCE, x), FREQUENCY, QUESTION], ascending=(False, False, True))
    return d.set_index(QUESTION)


def add_judgments_and_frequencies_to_qa_pairs(qa_pairs, judgments, question_frequencies, remove_newlines):
    """
    Collate system answer confidences and annotator judgments by question/answer pair.
    Add to each pair the question frequency.

    Though you expect the set of question/answer pairs in the system answers and judgments to not be disjoint, it may
    be the case that neither is a subset of the other. If annotation is incomplete, there may be Q/A pairs in the
    system answers that haven't been annotated yet. If multiple systems are being judged, there may be Q/A pairs in the
    judgements that don't appear in the system answers.

    Some versions of Annotation Assist strip newlines from the answers they return in the judgement files, so
    optionally take this into account when joining on question/answer pairs.

    :param qa_pairs: question, answer, and confidence provided by a Q&A system
    :type qa_pairs: pandas.DataFrame
    :param judgments: question, answer, in purview, and judgement provided by annotators
    :type judgments: pandas.DataFrame
    :param question_frequencies: question and question frequency in the test set
    :type question_frequencies: pandas.DataFrame
    :param remove_newlines: join judgments on answers with newlines removed
    :type remove_newlines: bool
    :return: question and answer pairs with confidence, in purview, judgement and question frequency
    :rtype: pandas.DataFrame
    """
    qa_pairs = pandas.merge(qa_pairs, question_frequencies, on=QUESTION, how="left")
    if remove_newlines:
        qa_pairs["Temp"] = qa_pairs[ANSWER].str.replace("\n", "")
        qa_pairs = qa_pairs.rename(columns={"Temp": ANSWER, ANSWER: "Temp"})
    qa_pairs = pandas.merge(qa_pairs, judgments, on=(QUESTION, ANSWER), how="left")
    if remove_newlines:
        del qa_pairs[ANSWER]
        qa_pairs = qa_pairs.rename(columns={"Temp": ANSWER})
    return qa_pairs


def drop_missing(systems_data):
    if any(systems_data.isnull()):
        n = len(systems_data)
        systems_data = systems_data.dropna()
        m = n - len(systems_data)
        logger.warning("Dropping %d of %d question/answer pairs missing information (%0.3f%%)" % (m, n, 100.0 * m / n))
    return systems_data


class CollatedFileType(CsvFileType):
    columns = [QUESTION, SYSTEM, ANSWER, CONFIDENCE, IN_PURVIEW, CORRECT, FREQUENCY]

    def __init__(self):
        super(self.__class__, self).__init__(CollatedFileType.columns)

    @staticmethod
    def output_format(collated):
        collated = collated[CollatedFileType.columns]
        collated = collated.sort_values([QUESTION, SYSTEM])
        return collated.set_index([QUESTION, SYSTEM, ANSWER])