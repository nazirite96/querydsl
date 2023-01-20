package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;
import javax.xml.bind.SchemaOutputResolver;

import java.util.List;

import static com.querydsl.core.types.Projections.*;
import static com.querydsl.jpa.JPAExpressions.*;
import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @PersistenceContext
    EntityManager em;

    @PersistenceUnit
    EntityManagerFactory emf;

    JPAQueryFactory query;

    @BeforeEach
    public void before() {
        query = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1",10,teamA);
        Member member2 = new Member("member2",20,teamA);
        Member member3 = new Member("member3",30,teamB);
        Member member4 = new Member("member4",40,teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);

        em.flush();
        em.clear();

    }


    @Test
    public void startJPQL() {

        //find member1
        String qlString = "select m from Member m " +
                "where m.username = :username";

        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");

    }


    @Test
    public void startQuerydsl() throws Exception{
        //member1을 찾아라
        JPAQueryFactory query = new JPAQueryFactory(em);
        QMember qMember = member;

        Member findMember = query.selectFrom(qMember).
                where(qMember.username.eq("member1")).
                fetchOne();

        assertThat(findMember.getUsername())
                .isEqualTo("member1");

    }


    @Test
    public void startQuerydsl2() {

        QMember m = member;

        Member findMember = query
                .select(m)
                .from(m)
                .where(m.username.eq("member1"))
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }


    @Test
    public void startQuerydsl3() {

        Member findMember = query.select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = query.select(member)
                .from(member)
                .where(member.username.eq("member1").and(member.age.eq(10)))
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

//        member.username.eq("member1") // username = 'member1'
//        member.username.ne("member1") //username != 'member1'
//        member.username.eq("member1").not() // username != 'member1'
//        member.username.isNotNull() //이름이 is not null
//        member.age.in(10, 20) // age in (10,20)
//        member.age.notIn(10, 20) // age not in (10, 20)
//        member.age.between(10,30) //between 10, 30
//        member.age.goe(30) // age >= 30
//        member.age.gt(30) // age > 30
//        member.age.loe(30) // age <= 30
//        member.age.lt(30) // age < 30
//        member.username.like("member%") //like 검색
//        member.username.contains("member") // like ‘%member%’ 검색
//        member.username.startsWith("member") //like ‘member%’ 검색

    @Test
    public void searchAndParam() {
        List<Member> result1 = query
                .selectFrom(member)
                .where(member.username.eq("member1"),member.age.eq(10))
                .fetch();
        assertThat(result1.size()).isEqualTo(1);
    }

    @Test
    public void resultFetch(){
        //List
        List<Member> fetch = query
                .selectFrom(member)
                .fetch();

        //단 건
        Member findMember1 = query
                .selectFrom(member)
                .fetchOne();
        //처음 한 건 조회
        Member findMember2 = query
                .selectFrom(member)
                .fetchFirst();
        //페이징에서 사용
        QueryResults<Member> results = query
                .selectFrom(member)
                .fetchResults();
        //count 쿼리로 변경
        long count = query
                .selectFrom(member)
                .fetchCount();

    }


    /**
     * 회원 정렬 순서
     * 1.회원 나이 내림차순(desc)
     * 2.회원 이름 올림차순(asc)
     * 단 2에서 회원이름이 없으면 마지막에 출력(nulls last)
     */
    @Test
    public void Sort() {
        em.persist(new Member(null,100));
        em.persist(new Member("member5",100));
        em.persist(new Member("member6",100));

        List<Member> result = query
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);
        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void paging1() {
        List<Member> result = query.selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();


        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2() {
        QueryResults<Member> queryResults = query
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();
        //이거 이제 쓰면 안됨
        // https://velog.io/@nestour95/QueryDsl-fetchResults%EA%B0%80-deprecated-%EB%90%9C-%EC%9D%B4%EC%9C%A0
        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults().size()).isEqualTo(2);

    }

    @Test
    public void aggregation() throws Exception {
        List<Tuple> result = query
                .select(member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();
        //tuple이란 여러게의 결과 타입을 반환 받을 수 있는 맵

        //보통은 dto를 사용하고

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);

    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구해라.
     * @throws Exception
     */
    @Test
    public void group() throws Exception {
        List<Tuple> result = query
                .select(team.name,member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
//                .having(team.name.eq("teamA"))
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }

    /**
     * 팀 A에 소속된 모든 회원
     * @throws Exception
     */

    @Test
    public void join() throws Exception {
        List<Member> result = query
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result).extracting("username")
                .containsExactly("member1","member2");
    }

    /**
     * 세타 조인(연관관계가 없는 필드로 조인)
     * 회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void theta_join() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Member> result = query
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA","teamB");

    }

    /**
     *  예) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     *  JPQL: SELECT m, t FROM Member m LEFT JOIN m.team t on t.name = 'teamA'
     *  SQL: SELECT m.*,t.* FROM Member m LEFT JOIN Team t ON m.TEAM_ID=t.id and t.name='teamA'
     */
    @Test
    public void join_on_filtering() throws Exception {
        List<Tuple> result = query
                .select(member, team)
                .from(member)
                .join(member.team, team)//.on(team.name.eq("teamA").and(member.team.eq(team)))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void join_on_no_relation() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Tuple> result = query
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("t=" + tuple);

        }

    }

    @Test
    public void fetchJoinNo() throws Exception {
        em.flush();
        em.clear();

        Member findMember = query
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded =
                emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isFalse();


    }

    @Test
    public void fetchJoinUse() throws Exception {
        em.flush();
        em.clear();

        Member findMember = query
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치조인 적용").isTrue();
        System.out.println(findMember);
        System.out.println(findMember.getTeam());
    }

    /**
     * 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery() throws Exception {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = query
                .selectFrom(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    @Test
    public void subQueryGoe() throws Exception {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = query
                .selectFrom(member)
                .where(member.age.goe(
                        select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(30,40);

    }

    @Test
    public void subQueryIn() throws Exception {
        QMember memberSub = new QMember("subMember");

//        List<Member> result = query
//                .selectFrom(member)
//                .where(member.age.in(
//                        JPAExpressions.select(memberSub.age)
//                                .from(memberSub)
//                                .where(memberSub.age.gt(10))
//                )).fetch();
//
//        assertThat(result).extracting("age")
//                .containsExactly(20,30,40);


        List<Tuple> fetch = query
                .select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub)
                ).from(member)
                .fetch();
        for (Tuple tuple : fetch) {
            System.out.println("username = " + tuple.get(member.username));
            System.out.println("age = " +
                    tuple.get(select(memberSub.age.avg())
                            .from(memberSub)));
        }

        
    }
    
    @Test
    public void basicCase() {

        List<Tuple> list = query.select(
                        member, member.age
                                .when(10).then("열살")
                                .when(20).then("스무살")
                                .otherwise("기타"))
                .from(member)
                .fetch();

        int i = 0;
        for (Tuple tuple : list) {
            System.out.println(tuple.get(member.age
                    .when(10).then("열살")
                    .when(20).then("스무살")
                    .otherwise("기타")));
        }
    }

    //데이터베이스는 데이트 워커가 아니다.
    @Test
    public void complexCase(){
        List<String> result = query.select(new CaseBuilder()
                .when(member.age.between(0, 20)).then("0~20")
                .when(member.age.between(21, 40)).then("21~40")
                .otherwise("기타")
        ).from(member)
        .fetch();

        for (String s : result) {
            System.out.println(s);
        }

    }

    @Test
    public void concat() {

        List<String> result = query
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println(s);
        }
    }

    @Test
    @DisplayName("")
    public void simpleProjection() throws Exception {
        // given
        List<MemberDto> resultSetter = query.select(bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
        // when
        for (MemberDto memberDto : resultSetter) {
            System.out.println(memberDto);
        }

        // then
    }
    
    @Test
    @DisplayName("")
    public void tupleProjection() throws Exception {
        // given
        List<Tuple> result = query.select(member.username, member.age)
                .from(member)
                .fetch();
        // when
        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
        // then
    }
    
    @Test
    @DisplayName("")
    public void findDtoByJpql() throws Exception {
        List<MemberDto> results = em.createQuery(
                "select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m "
                , MemberDto.class).getResultList();

        for (MemberDto result : results) {
            System.out.println(result);
        }
    }

    @Test
    @DisplayName("")
    public void findByDtoSetter() throws Exception {
        // given
        List<MemberDto> resultSetter = query.select(bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
        // when
        for (MemberDto memberDto : resultSetter) {
            System.out.println("memberDto = " + memberDto);
        }
        // then
    }

    @Test
    @DisplayName("")
    public void findByDtoFeild() throws Exception {
        // given
        List<MemberDto> resultSetter = query.select(fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
        // when
        for (MemberDto memberDto : resultSetter) {
            System.out.println("memberDto = " + memberDto);
        }
        // then
    }

    @Test
    @DisplayName("")
    public void findByDtoConstructor() throws Exception {
        // given
        List<MemberDto> result = query.select(constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
        // when

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
        // then
    }


    @Test
    @DisplayName("")
    public void QuerydslBasicTest() throws Exception {
        QMember memberSub = new QMember("subMember");

        List<UserDto> resultOtherDto = query.select(
                        fields(UserDto.class,
                                member.username.as("name"),
                                ExpressionUtils.as(
                                        JPAExpressions
                                                .select(memberSub.age.max())
                                                .from(memberSub),"age")
                        )
                ).from(member)
                .fetch();
    }
    
    @Test
    @DisplayName("")
    public void findQueryByConstrotor() throws Exception {
        List<MemberDto> resultConst = query
                .select(constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : resultConst) {
            System.out.println("memberDto = " + memberDto);
        }

    }
    @Test
    @DisplayName("")
    public void findDtoByQueryProjection() throws Exception {
        // given
        List<MemberDto> resultQureyProjection = query
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();
        // when
        for (MemberDto memberDto : resultQureyProjection) {
            System.out.println("memberDto = " + memberDto);
        }
        // then
    }

    @Test
    @DisplayName("")
    public void distinct() throws Exception {
        // given
        List<String> resultDistinct = query
                .select(member.username).distinct()
                .from(member)
                .fetch();
        // then
    }


    //--------------------------------------------------
    @Test
    @DisplayName("")
    public void dynamic_BooleanBuilder() throws Exception{
        // given
        String usernameParam = "member1";
        Integer ageParam = 10;
        // when

        List<Member> result = searchMember1(usernameParam, ageParam);

        // then
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {
        BooleanBuilder builder = new BooleanBuilder();
        if(usernameCond != null){
            builder.and(member.username.eq(usernameCond));
        }
        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }
        return query.selectFrom(member)
                .where(builder)
                .fetch();

    }

    @Test
    @DisplayName("")
    public void dynamic_WhereParam() throws Exception {
        // given
        String usernameParam = "member1";
        Integer ageParam = 10;
        // when

        List<Member> result = searchMember2(usernameParam, ageParam);
        // then

    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return query
                .selectFrom(member)
                .where(usernameEq(usernameCond),ageEq(ageCond))
                .fetch();
    }

    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond != null ? member.age.eq(ageCond) : null;
    }

    private BooleanExpression usernameEq(String usernameCond) {
        return usernameCond != null ? member.username.eq(usernameCond) : null;
    }

    @Test
    @DisplayName("")
    public void bulkUpdate() throws Exception {
        // given
        long count = query.update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();
        // when

        // then
    }

    @Test
    @DisplayName("")
    public void bulkAdd() throws Exception {
        // given
        long count = query.update(member)
                .set(member.age, member.age.add(1))
                .execute();
        // when

        // then
    }
    @Test
    @DisplayName("")
    public void bulkDelete() throws Exception {
        // given
        long count = query.delete(member)
                .where(member.age.gt(18))
                .execute();
        // when
        
        // then
    }
    
    @Test
    @DisplayName("")
    public void sqlFunctnion() throws Exception {

        List<String> result = query.select(
                        Expressions.stringTemplate(
                                "function('repace', {0}, {1}, {2})",
                                member.username, "member", "M"
                        )
                ).from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }

    }
    @Test
    @DisplayName("")
    public void sqlFunctnion2() throws Exception {

//        List<String> result = query
//                .select(member.username)
//                .from(member)
//                .where(member.username.eq(
//                        Expressions.stringTemplate(
//                                "function('lower', {0})",
//                                member.username.)))
//                .fetch();
//
//        for (String s : result) {
//            System.out.println("s = " + s);
//        }

    }


}