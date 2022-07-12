/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pentaho.di.sdk.delparamo.csv2rdf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

public class Mapper {

  private String uri;
  private String user;
  private String pass;
  private String context;

  private String[] variables;
  private String mapping;
  private Repository repositoryMem;
  private SPARQLRepository repositoryOut;

  private String lastRDF;

  private Model buffer;

  private RepositoryConnection mem;
  private GraphQuery prepareGraphQuery;

  public Mapper(String uri, String user, String pass, String context, String[] variables, String mapping) {
    this.uri = uri;
    this.user = user;
    this.pass = pass;
    this.context = context;
    this.variables = variables;
    this.mapping = mapping;
  }

  public void init() {
    repositoryMem = new SailRepository(new MemoryStore());
    repositoryMem.init();
    repositoryOut = new SPARQLRepository(this.uri);
    repositoryOut.setUsernameAndPassword(this.user, this.pass);
    Map<String, String> additionalHttpHeaders = new ConcurrentHashMap<>();
    additionalHttpHeaders.put("Accept", "application/sparql-results+json,*/*;q=0.9");
    repositoryOut.setAdditionalHttpHeaders(additionalHttpHeaders);
    repositoryOut.init();
    buffer = new LinkedHashModel();
    mem = repositoryMem.getConnection();
    prepareGraphQuery = mem.prepareGraphQuery(QueryLanguage.SPARQL, this.mapping);
  }

  public void end() {
    this.dump();
    mem.close();
    repositoryMem.shutDown();
    repositoryOut.shutDown();
  }

  public void map(Object[] data) throws IOException {
    prepareGraphQuery.clearBindings();
    for (int i = 0; i < variables.length; i++) {
      if (data[i] != null) {
        prepareGraphQuery.setBinding(this.variables[i], SimpleValueFactory.getInstance().createLiteral(data[i].toString()));
      }
    }
    GraphQueryResult evaluate = prepareGraphQuery.evaluate();
    Model md = new LinkedHashModel();
    while (evaluate.hasNext()) {
      md.add(evaluate.next());
    }
    try (ByteArrayOutputStream o = new ByteArrayOutputStream()) {
      Rio.write(md, o, RDFFormat.NTRIPLES);
      this.lastRDF = o.toString();
    }
    this.buffer.addAll(md);
    if (this.buffer.size() > 2000) {
      dump();
    }

  }

  public void dump() {
    try (RepositoryConnection rem = repositoryOut.getConnection()) {
      rem.begin();
      rem.add(this.buffer, SimpleValueFactory.getInstance().createIRI(this.context));
      rem.commit();
      this.buffer.clear();
    } catch (Exception e){
      Rio.write(buffer, System.out, RDFFormat.RDFXML);
    }
  }

  public String getContext() {
    return context;
  }

  public String getLastRDF() {
    return lastRDF;
  }

  public void setLastRDF(String lastRDF) {
    this.lastRDF = lastRDF;
  }

  public void setContext(String context) {
    this.context = context;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPass() {
    return pass;
  }

  public void setPass(String pass) {
    this.pass = pass;
  }

  public Repository getRepositoryMem() {
    return repositoryMem;
  }

  public void setRepositoryMem(Repository repositoryMem) {
    this.repositoryMem = repositoryMem;
  }

  public SPARQLRepository getRepositoryOut() {
    return repositoryOut;
  }

  public void setRepositoryOut(SPARQLRepository repositoryOut) {
    this.repositoryOut = repositoryOut;
  }

  public String[] getVariables() {
    return variables;
  }

  public void setVariables(String[] variables) {
    this.variables = variables;
  }

  public String getMapping() {
    return mapping;
  }

  public void setMapping(String mapping) {
    this.mapping = mapping;
  }

}
